// The owner's background-task commands (DD-09 §1), one transaction each:
// the domain decision under the row lock of the generation, the attempt
// record, the CAS update and the outbox event on the same client. No
// network, queue or NATS I/O happens while a row is locked; the
// original-dispatch query of an external-effect lease runs before the
// transaction that consumes its answer.
import * as db from "../adapters/postgres.js";
import { completedEvent, requestedEvent } from "../domain/event.js";
import {
	type Attempt,
	attemptOutcome,
	type Bounds,
	type DispatchOutcome,
	decideCancel,
	decideClaim,
	decideExpire,
	decideHeartbeat,
	decideSubmit,
	newRequest,
	type Submission,
	type SubmitDecision,
	supersede,
	type Task,
	TaskError,
	terminal,
} from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";

export interface Clock {
	now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };

/** Asks Control about the original dispatch of an external-effect attempt; the disabled one answers unknown. */
export interface DispatchQuery {
	outcome(dispatchId: string): Promise<DispatchOutcome>;
}

export const noDispatchQuery: DispatchQuery = { outcome: async () => "unknown" };

export interface RequestInput {
	taskId: string;
	tenantId: string;
	kind: string;
	profile: string;
	input: string;
	effects: Task["effects"];
	dispatchId: string;
	authorizationRef: string;
	correlationId: string;
}

export class Tasks {
	private bounds: Bounds;
	private expiresAt?: number;
	setExpiry(expiry?: number): void {
		this.expiresAt = expiry;
	}
	ready(): boolean {
		return this.expiresAt === undefined || this.clock.now().getTime() < this.expiresAt;
	}
	private admitConfiguration(): void {
		if (!this.ready()) throw new TaskError("STALE_EXECUTION", "active configuration expired");
	}

	constructor(
		private readonly store: db.Store,
		private readonly dispatch: DispatchQuery,
		bounds: Bounds,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {
		this.bounds = bounds;
	}

	/** The bounds of the active generation (replaced whole; they govern new admissions only). */
	setBounds(b: Bounds): void {
		this.bounds = b;
	}

	/**
	 * Commits the durable request and its background.requested event in one
	 * transaction. The same task with the same input returns the existing
	 * generation; a different input creates the next generation and fences
	 * the previous one.
	 */
	async request(input: RequestInput): Promise<{ task: Task; existing: boolean }> {
		const out = await this.store.inTx(async (c) => {
			this.admitConfiguration();
			if (Buffer.byteLength(input.input) > this.bounds.maxInputBytes)
				throw new TaskError(
					"INPUT_TOO_LARGE",
					`${Buffer.byteLength(input.input)} bytes over the ${this.bounds.maxInputBytes}-byte bound`,
				);
			let canonical: string;
			try {
				canonical = await db.canonicalJson(c, input.input);
			} catch {
				throw new TaskError("INVALID_ARGUMENT", "input is not JSON");
			}
			const candidate = newRequest({ ...input, input: canonical }, this.bounds);
			const now = this.clock.now();
			const latest = await db.getLatestRequest(c, input.taskId, true);
			if (latest) {
				if (latest.tenantId !== input.tenantId)
					throw new TaskError("FORBIDDEN", `task ${input.taskId} belongs to another tenant`);
				if (latest.inputDigest === candidate.inputDigest) return { task: latest, existing: true };
				candidate.generation = latest.generation + 1;
				const fenced = supersede(latest);
				if (fenced.changed)
					await this.apply(c, fenced.task, latest.revision, now, latest.state === "leased" ? attemptOutcome.stale : "");
			}
			if ((await db.sourceAuthorization(c, candidate.authorizationRef, candidate.tenantId)) !== "current")
				throw new TaskError("FORBIDDEN", `${candidate.authorizationRef} is not a current authorization`);
			this.admitConfiguration();
			await db.insertRequest(c, candidate);
			await db.publishOutbox(c, requestedEvent(candidate, now));
			return { task: candidate, existing: false };
		});
		this.log.info("background request committed", {
			taskId: out.task.taskId,
			generation: out.task.generation,
			kind: out.task.kind,
			existing: out.existing,
		});
		return out;
	}

	private async apply(c: db.PoolClient, task: Task, readRevision: number, now: Date, outcome: string): Promise<void> {
		const n = await db.updateRequest(c, task, readRevision, now);
		if (n !== 1)
			throw new TaskError("STALE_EXECUTION", `generation ${task.generation} of ${task.taskId} changed under the lock`);
		if (outcome && task.attemptCount > 0)
			await db.setAttemptOutcome(c, task.taskId, task.generation, task.attemptCount, outcome, now);
		if (terminal(task.state)) await db.publishOutbox(c, completedEvent(task, now));
	}

	private static current(task: Task, attempts: Attempt[]): Attempt | undefined {
		return attempts.find((a) => a.ordinal === task.attemptCount);
	}

	private async locked(
		c: db.PoolClient,
		taskId: string,
		generation: number,
	): Promise<{ task: Task; attempts: Attempt[] }> {
		const task = await db.getRequestForUpdate(c, taskId, generation);
		if (!task) throw new TaskError("NOT_FOUND", `${taskId} generation ${generation}`);
		return { task, attempts: await db.listAttempts(c, taskId, generation) };
	}

	async claim(
		taskId: string,
		generation: number,
		workerId: string,
		leaseMs: number,
	): Promise<{ task: Task; input: string }> {
		try {
			const out = await this.store.inTx(async (c) => {
				this.admitConfiguration();
				const { task, attempts } = await this.locked(c, taskId, generation);
				const now = this.clock.now();
				this.admitConfiguration();
				const d = decideClaim(task, attempts, workerId, leaseMs, now, this.bounds);
				if (d.expiredPrevious) {
					this.metrics.leaseOverruns.inc();
					await db.setAttemptOutcome(c, taskId, generation, task.attemptCount, attemptOutcome.expired, now);
				}
				await this.apply(c, d.task, task.revision, now, "");
				await db.insertAttempt(c, taskId, generation, d.attempt);
				return { task: d.task, input: task.input };
			});
			this.metrics.claims.inc({ outcome: "accepted" });
			return out;
		} catch (err) {
			const code = err instanceof TaskError ? err.code : isUniqueViolation(err) ? "ALREADY_CLAIMED" : "";
			this.metrics.claims.inc({
				outcome:
					code === "ALREADY_CLAIMED"
						? "refused_claimed"
						: code === "WORKER_IDENTITY_REUSED"
							? "refused_identity"
							: code === "EFFECT_UNCERTAIN"
								? "refused_effect_uncertain"
								: "refused",
			});
			if (isUniqueViolation(err)) throw new TaskError("ALREADY_CLAIMED", "concurrent claim");
			throw err;
		}
	}

	async heartbeat(taskId: string, generation: number, workerId: string): Promise<Task> {
		return this.store.inTx(async (c) => {
			const { task, attempts } = await this.locked(c, taskId, generation);
			const now = this.clock.now();
			const extended = decideHeartbeat(task, Tasks.current(task, attempts), workerId, now);
			await this.apply(c, extended, task.revision, now, "");
			return extended;
		});
	}

	async submit(taskId: string, generation: number, sub: Submission): Promise<SubmitDecision> {
		let decision: SubmitDecision;
		try {
			decision = await this.store.inTx(async (c) => {
				const { task, attempts } = await this.locked(c, taskId, generation);
				const auth = await db.sourceAuthorization(c, task.authorizationRef, task.tenantId);
				const now = this.clock.now();
				const d = decideSubmit(task, Tasks.current(task, attempts), sub, auth, now, this.bounds);
				if (d.changed) await this.apply(c, d.task, task.revision, now, d.attemptOutcome);
				return d;
			});
		} catch (err) {
			const code = err instanceof TaskError ? err.code : "";
			this.metrics.submissions.inc({
				outcome:
					code === "STALE_EXECUTION" ? "refused_stale" : code === "INVALID_ARGUMENT" ? "refused_invalid" : "refused",
			});
			throw err;
		}
		this.metrics.submissions.inc({
			outcome: decision.existing
				? "existing"
				: decision.accepted
					? "accepted"
					: decision.task.state === "canceled"
						? "revoked"
						: decision.task.failureCode === "PROFILE_MISMATCH"
							? "profile_mismatch"
							: "failed",
		});
		return decision;
	}

	async get(taskId: string): Promise<Task> {
		const task = await db.getLatestRequest(this.store.pool, taskId);
		if (!task) throw new TaskError("NOT_FOUND", taskId);
		return { ...task, input: "" };
	}

	async cancel(taskId: string): Promise<{ task: Task; changed: boolean }> {
		return this.store.inTx(async (c) => {
			const latest = await db.getLatestRequest(c, taskId, true);
			if (!latest) throw new TaskError("NOT_FOUND", taskId);
			const d = decideCancel(latest);
			if (d.changed) await this.apply(c, d.task, latest.revision, this.clock.now(), d.attemptOutcome);
			return { task: d.task, changed: d.changed };
		});
	}

	/** Settles leases that ran out; external-effect leases are queried at Control before the transaction. */
	async sweepExpired(limit: number): Promise<number> {
		const expired = await db.listExpiredLeases(this.store.pool, this.clock.now(), limit);
		let settled = 0;
		for (const e of expired) {
			const task = await db.getRequest(this.store.pool, e.taskId, e.generation);
			if (!task) continue;
			let outcome: DispatchOutcome = "unknown";
			if (task.effects === "external") {
				try {
					outcome = await this.dispatch.outcome(task.dispatchId);
					this.metrics.dispatchQueries.inc({ answer: outcome });
				} catch (err) {
					this.metrics.dispatchQueries.inc({ answer: "error" });
					this.log.warn("original dispatch query failed; the lease stays unreleased", {
						taskId: task.taskId,
						generation: task.generation,
						error: String(err),
					});
					continue;
				}
			}
			try {
				await this.store.inTx(async (c) => {
					const current = await db.getRequestForUpdate(c, e.taskId, e.generation);
					if (!current) return;
					const now = this.clock.now();
					const d = decideExpire(current, outcome, now, this.bounds);
					if (!d.changed) return;
					settled++;
					this.metrics.leaseOverruns.inc();
					await this.apply(c, d.task, current.revision, now, d.attemptOutcome);
				});
			} catch (err) {
				this.log.warn("lease expiry not settled", { taskId: e.taskId, generation: e.generation, error: String(err) });
			}
		}
		return settled;
	}

	async observe(consumerGroup: string): Promise<void> {
		try {
			const o = await db.observe(this.store.pool, consumerGroup, this.clock.now());
			for (const s of [
				"pending",
				"leased",
				"result_submitted",
				"accepted",
				"retry_scheduled",
				"dead",
				"stale",
				"canceled",
			])
				this.metrics.requests.set({ state: s }, o.byState[s] ?? 0);
			this.metrics.overdueRetries.set(o.overdueRetries);
			this.metrics.outboxOldest.set(o.oldestUnforwardedSeconds);
			this.metrics.poolTotal.set(this.store.pool.totalCount);
			this.metrics.poolIdle.set(this.store.pool.idleCount);
		} catch (err) {
			this.log.warn("observation failed", { error: String(err) });
		}
	}
}

function isUniqueViolation(err: unknown): boolean {
	return typeof err === "object" && err !== null && (err as { code?: string }).code === "23505";
}
