// MemoryService (DD-07 §5, SEC-06): MemoryFact proposals and decisions, one
// transaction each. A proposal records who it speaks for (a user, a model
// or a worker) and its exact provenance; only an authorized user decides
// (confirm, reject, revoke) under the expected revision, and an identity
// that speaks for a model or a worker never decides. A confirmation binds
// the provenance the decider can read now at its current revision, the
// expiry and the conflict rule (one live confirmed fact per content,
// subject and scope). The fact, its append-only decision, its outbox event
// and the durable memory-project requests of every projection target
// commit together; the projections (the PostgresStore and every vector
// generation) follow from those requests and never decide anything.
import * as idx from "../adapters/indexdb.js";
import * as mdb from "../adapters/memorydb.js";
import * as db from "../adapters/postgres.js";
import * as rdb from "../adapters/retrievaldb.js";
import { memoryFactEvent } from "../domain/event.js";
import { type SpaceProfile, sameSpace } from "../domain/index.js";
import {
	checkDecider,
	contentDigestOf,
	type Decision,
	expiryPolicy,
	type Fact,
	type FactOrigin,
	factIdOf,
	MemoryError,
	normalizeRefs,
	type ProjectionAction,
	type ProjectionInput,
	parseSourceRef,
	projectionProfile,
	projectionTaskIdOf,
	storeTarget,
	subjectScopeOf,
	transition,
	visible,
} from "../domain/memory.js";
import { type Command, checkCommand, principalsOf, type Scope, SourceError } from "../domain/source.js";
import { digestOf } from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import type { Clock, Tasks } from "./tasks.js";

export interface MemoryBounds {
	maxAllowedFacts: number;
	maxProjectionEpochs: number;
	retryDelayMs: number;
}

export interface Proposal {
	subjectType: string;
	subjectId: string;
	content: string;
	sourceRefs: string[];
	expiresAt: Date | null;
	origin: FactOrigin;
}

function commandOf(cmd: Command, scope: Scope): void {
	try {
		checkCommand(cmd, scope);
	} catch (err) {
		if (err instanceof SourceError)
			throw new MemoryError(err.code === "FORBIDDEN" ? "FORBIDDEN" : "INVALID_ARGUMENT", err.message);
		throw err;
	}
}

function uniqueViolation(err: unknown): string | undefined {
	const e = err as { code?: string; constraint?: string };
	return e?.code === "23505" ? (e.constraint ?? "") : undefined;
}

export class Memory {
	constructor(
		protected readonly store: db.Store,
		protected readonly tasks: Tasks,
		protected readonly space: () => SpaceProfile | undefined,
		protected readonly bounds: () => MemoryBounds,
		protected readonly clock: Clock,
		protected readonly log: Logger,
		protected readonly metrics: Metrics,
	) {}

	// ---------------------------------------------------------------------
	// Provenance
	// ---------------------------------------------------------------------

	/**
	 * Every reference must name a source the scope can read now, at that
	 * source's current revision. An unreadable source is indistinguishable
	 * from a stale reference.
	 */
	protected async checkProvenance(c: db.PoolClient | undefined, scope: Scope, refs: string[]): Promise<void> {
		if (refs.length === 0) return;
		const parsed = refs.map(parseSourceRef);
		const readable = await rdb.readableNow(
			c ?? this.store.pool,
			scope.tenantId,
			scope.projectId,
			[...principalsOf(scope)],
			[...new Set(parsed.map((r) => r.sourceId))],
		);
		for (const r of parsed)
			if (readable.get(r.sourceId)?.currentRevision !== r.revision)
				throw new MemoryError("PROVENANCE_STALE", "a source reference is not readable at its current revision");
	}

	// ---------------------------------------------------------------------
	// Commands
	// ---------------------------------------------------------------------

	private async replay(
		cmd: Command,
		kind: mdb.DecisionRow["decision"],
		factId?: string,
	): Promise<{ fact: Fact; existing: true } | undefined> {
		const rec = await mdb.decisionByCommand(this.store.pool, cmd.tenantId, cmd.commandId);
		if (!rec) return undefined;
		if (rec.decision !== kind || rec.requestDigest !== cmd.requestDigest || (factId && rec.factId !== factId))
			throw new MemoryError("COMMAND_CONFLICT", "the command id was used with another request");
		const fact = await mdb.getFact(this.store.pool, rec.factId);
		if (!fact) throw new MemoryError("NOT_FOUND", "fact");
		return { fact, existing: true };
	}

	async propose(cmd: Command, scope: Scope, p: Proposal): Promise<{ fact: Fact; existing: boolean }> {
		commandOf(cmd, scope);
		if (!["user", "model", "worker"].includes(p.origin)) throw new MemoryError("INVALID_ARGUMENT", "origin");
		const scopeId = subjectScopeOf(scope, p.subjectType, p.subjectId);
		if (!p.content || p.content.length > 4096) throw new MemoryError("INVALID_ARGUMENT", "content");
		const refs = normalizeRefs(p.sourceRefs);
		const replayed = await this.replay(cmd, "propose");
		if (replayed) return replayed;
		const now = this.clock.now();
		if (p.expiresAt && p.expiresAt.getTime() <= now.getTime())
			throw new MemoryError("EXPIRED", "the proposed expiry has passed");
		await this.checkProvenance(undefined, scope, refs);
		const fact: Fact = {
			factId: factIdOf(cmd.tenantId, cmd.commandId),
			tenantId: scope.tenantId,
			subjectType: p.subjectType as Fact["subjectType"],
			subjectId: p.subjectId,
			scopeId,
			content: p.content,
			contentDigest: contentDigestOf(p.content),
			state: "proposed",
			revision: 1,
			proposer: scope.actorId,
			confirmer: "",
			origin: p.origin,
			sourceRefs: refs,
			expiresAt: p.expiresAt,
			deleted: false,
			deletedAt: null,
			purgedAt: null,
			createdAt: now,
			updatedAt: now,
		};
		try {
			await this.store.inTx(async (c) => {
				await mdb.insertFact(c, fact, cmd.commandId, cmd.requestDigest);
				await mdb.insertDecision(c, {
					decisionId: `${fact.factId}-r1`,
					factId: fact.factId,
					tenantId: fact.tenantId,
					fromRevision: 0,
					toRevision: 1,
					decision: "propose",
					decider: scope.actorId,
					authority: p.origin,
					policyRevision: "",
					reasonCode: "",
					sourceRefs: refs,
					expiresAt: p.expiresAt,
					commandId: cmd.commandId,
					requestDigest: cmd.requestDigest,
				});
			});
		} catch (err) {
			// A concurrent identical command committed first: answer with it.
			if (uniqueViolation(err) !== undefined) {
				const again = await this.replay(cmd, "propose");
				if (again) return again;
			}
			throw err;
		}
		this.metrics.memoryDecisions.inc({ decision: "propose", origin: p.origin });
		this.log.info("memory fact proposed", { factId: fact.factId, origin: p.origin });
		return { fact: (await mdb.getFact(this.store.pool, fact.factId)) ?? fact, existing: false };
	}

	async decide(
		cmd: Command,
		scope: Scope,
		factId: string,
		expectedRevision: number,
		decision: Decision,
		reasonCode: string,
		expiresAt: Date | null,
	): Promise<{ fact: Fact; existing: boolean }> {
		commandOf(cmd, scope);
		if (expiresAt && decision !== "confirm")
			throw new MemoryError("INVALID_ARGUMENT", "only a confirmation sets an expiry");
		const replayed = await this.replay(cmd, decision, factId);
		if (replayed) return replayed;
		let fact: Fact;
		try {
			fact = await this.store.inTx(async (c) => {
				const f = await mdb.getFact(c, factId, true);
				if (!f || !visible(f, scope)) throw new MemoryError("NOT_FOUND", "fact");
				checkDecider(f, scope.actorId, await mdb.speaksForModel(c, scope.tenantId, scope.actorId));
				if (f.revision !== expectedRevision)
					throw new MemoryError("REVISION_MISMATCH", `fact revision is ${f.revision}`);
				const next: Fact = { ...f, state: transition(f.state, decision), revision: f.revision + 1 };
				const now = this.clock.now();
				if (decision === "confirm") {
					await this.checkProvenance(c, scope, f.sourceRefs);
					const expiry = expiresAt ?? f.expiresAt;
					if (expiry && expiry.getTime() <= now.getTime())
						throw new MemoryError("EXPIRED", "the fact's expiry has passed");
					if (await mdb.confirmedDuplicate(c, f))
						throw new MemoryError("FACT_CONFLICT", "a confirmed fact with this content exists for the subject");
					next.confirmer = scope.actorId;
					next.expiresAt = expiry;
				}
				if ((await mdb.updateFact(c, next, f.revision)) !== 1)
					throw new MemoryError("REVISION_MISMATCH", "the fact changed under the lock");
				await mdb.insertDecision(c, {
					decisionId: `${f.factId}-r${next.revision}`,
					factId: f.factId,
					tenantId: f.tenantId,
					fromRevision: f.revision,
					toRevision: next.revision,
					decision,
					decider: scope.actorId,
					authority: "user",
					policyRevision: "",
					reasonCode,
					sourceRefs: f.sourceRefs,
					expiresAt: next.expiresAt,
					commandId: cmd.commandId,
					requestDigest: cmd.requestDigest,
				});
				if (decision !== "reject")
					await db.publishOutbox(
						c,
						memoryFactEvent(next, decision === "confirm" ? "confirmed" : "revoked", cmd.commandId, now),
					);
				await this.scheduleIn(c, next);
				return next;
			});
		} catch (err) {
			const constraint = uniqueViolation(err);
			if (constraint === "memory_facts_one_confirmed")
				throw new MemoryError("FACT_CONFLICT", "a confirmed fact with this content exists for the subject");
			if (constraint !== undefined) {
				const again = await this.replay(cmd, decision, factId);
				if (again) return again;
			}
			throw err;
		}
		this.metrics.memoryDecisions.inc({ decision, origin: "user" });
		this.log.info("memory fact decided", { factId, decision, revision: fact.revision });
		return { fact: (await mdb.getFact(this.store.pool, factId)) ?? fact, existing: false };
	}

	/**
	 * Ends readability in one transaction: the content is erased, the
	 * deletion is a decision, and every projection target receives a
	 * tombstone request. The digest and the decision history remain as the
	 * minimal audit evidence; purged_at is set once every target applied it.
	 */
	async delete(
		cmd: Command,
		scope: Scope,
		factId: string,
		expectedRevision: number,
	): Promise<{ fact: Fact; existing: boolean }> {
		commandOf(cmd, scope);
		const replayed = await this.replay(cmd, "delete", factId);
		if (replayed) return replayed;
		let fact: Fact;
		try {
			fact = await this.store.inTx(async (c) => {
				const f = await mdb.getFact(c, factId, true);
				if (!f || !visible(f, scope)) throw new MemoryError("NOT_FOUND", "fact");
				checkDecider(f, scope.actorId, await mdb.speaksForModel(c, scope.tenantId, scope.actorId));
				if (f.revision !== expectedRevision)
					throw new MemoryError("REVISION_MISMATCH", `fact revision is ${f.revision}`);
				const now = this.clock.now();
				const next: Fact = { ...f, revision: f.revision + 1, deleted: true, deletedAt: now, content: "" };
				if ((await mdb.updateFact(c, next, f.revision)) !== 1)
					throw new MemoryError("REVISION_MISMATCH", "the fact changed under the lock");
				await mdb.insertDecision(c, {
					decisionId: `${f.factId}-r${next.revision}`,
					factId: f.factId,
					tenantId: f.tenantId,
					fromRevision: f.revision,
					toRevision: next.revision,
					decision: "delete",
					decider: scope.actorId,
					authority: "user",
					policyRevision: "",
					reasonCode: "",
					sourceRefs: f.sourceRefs,
					expiresAt: f.expiresAt,
					commandId: cmd.commandId,
					requestDigest: cmd.requestDigest,
				});
				if (f.state === "confirmed") await db.publishOutbox(c, memoryFactEvent(next, "deleted", cmd.commandId, now));
				await this.scheduleIn(c, next);
				// Never projected: nothing to clear.
				await mdb.markPurgedIfComplete(c, f.factId);
				return next;
			});
		} catch (err) {
			if (uniqueViolation(err) !== undefined) {
				const again = await this.replay(cmd, "delete", factId);
				if (again) return again;
			}
			throw err;
		}
		this.metrics.memoryDecisions.inc({ decision: "delete", origin: "user" });
		this.log.info("memory fact deleted", { factId, revision: fact.revision });
		return { fact: (await mdb.getFact(this.store.pool, factId)) ?? fact, existing: false };
	}

	/**
	 * The reviewed expiry rule (memory-expiry-v1): a confirmed fact whose
	 * expiry passed becomes expired in its own transaction, as a policy
	 * decision recorded with the rule's revision, its event and the
	 * tombstone requests of every target. Recall already hides it from the
	 * moment its expiry passes; this makes the state and projections follow.
	 */
	async expireDue(limit: number): Promise<number> {
		let expired = 0;
		for (const factId of await mdb.expiredFacts(this.store.pool, this.clock.now(), limit)) {
			const done = await this.store.inTx(async (c) => {
				const f = await mdb.getFact(c, factId, true);
				const now = this.clock.now();
				if (!f || f.state !== "confirmed" || f.deleted || !f.expiresAt || f.expiresAt.getTime() > now.getTime())
					return false;
				const next: Fact = { ...f, state: "expired", revision: f.revision + 1 };
				if ((await mdb.updateFact(c, next, f.revision)) !== 1) return false;
				const commandId = `${expiryPolicy.id}:${f.factId}:r${next.revision}`;
				await mdb.insertDecision(c, {
					decisionId: `${f.factId}-r${next.revision}`,
					factId: f.factId,
					tenantId: f.tenantId,
					fromRevision: f.revision,
					toRevision: next.revision,
					decision: "expire",
					decider: `policy:${expiryPolicy.id}`,
					authority: "policy",
					policyRevision: expiryPolicy.revision,
					reasonCode: "EXPIRED",
					sourceRefs: f.sourceRefs,
					expiresAt: f.expiresAt,
					commandId,
					requestDigest: digestOf(commandId),
				});
				await db.publishOutbox(c, memoryFactEvent(next, "expired", expiryPolicy.id, now));
				await this.scheduleIn(c, next);
				return true;
			});
			if (done) {
				expired++;
				this.metrics.memoryDecisions.inc({ decision: "expire", origin: "policy" });
			}
		}
		if (expired > 0) this.log.info("memory facts expired by the reviewed rule", { count: expired });
		return expired;
	}

	async get(scope: Scope, factId: string): Promise<Fact> {
		const f = await mdb.getFact(this.store.pool, factId);
		// An unreadable fact is indistinguishable from a missing one.
		if (!f || !visible(f, scope)) throw new MemoryError("NOT_FOUND", "fact");
		return f;
	}

	async list(
		scope: Scope,
		filter: { subjectType: string; subjectId: string; state: string },
		cursor: string,
		limit: number,
	): Promise<{ facts: Fact[]; nextCursor: string }> {
		const pageSize = limit === 0 ? 50 : limit;
		let after = "";
		if (cursor) {
			after = Buffer.from(cursor, "base64url").toString("utf8");
			if (!/^mem-[0-9a-f]{32}$/.test(after)) throw new MemoryError("INVALID_ARGUMENT", "cursor");
		}
		const rows = await mdb.listVisible(this.store.pool, scope, filter, after, pageSize + 1);
		const page = rows.slice(0, pageSize);
		const last = page[page.length - 1];
		return {
			facts: page,
			nextCursor: rows.length > pageSize && last ? Buffer.from(last.factId).toString("base64url") : "",
		};
	}

	// ---------------------------------------------------------------------
	// Projection requests (inside the deciding transaction)
	// ---------------------------------------------------------------------

	/**
	 * The durable memory-project requests of a fact's new revision: a
	 * confirmed, undeleted fact is applied to the Store and to every open
	 * vector generation of the active space; any other state removes it from
	 * every target it was ever projected to (retired generations included).
	 * A fact that was never projected needs nothing.
	 */
	async scheduleIn(c: db.PoolClient, f: Fact): Promise<number> {
		const rows = await mdb.projectionsOf(c, f.factId, true);
		const apply = f.state === "confirmed" && !f.deleted;
		const targets = new Set(rows.map((r) => r.target));
		if (apply) {
			targets.add(storeTarget);
			const space = this.space();
			for (const g of await idx.listGenerations(c))
				if (g.state !== "retired" && space && sameSpace(g, space)) targets.add(g.generation);
		} else if (rows.length === 0) return 0;
		else targets.add(storeTarget);
		for (const target of [...targets].sort((a, b) => a - b))
			await this.requestIn(c, f, target, apply ? "apply" : "remove", rows.find((r) => r.target === target)?.epoch ?? 0);
		return targets.size;
	}

	protected requestIn(
		c: db.PoolClient,
		f: Pick<Fact, "factId" | "revision" | "tenantId">,
		target: number,
		action: ProjectionAction,
		epoch: number,
	): Promise<void> {
		return requestProjectionIn(this.tasks, c, f, target, action, epoch);
	}
}

/**
 * Points the (fact, target) ledger row at the fact's revision and commits
 * the memory-project request that carries it, inside the caller's
 * transaction. A new revision, action or epoch is a new input: the task's
 * previous generation is fenced (stale) by the request itself.
 */
export async function requestProjectionIn(
	tasks: Tasks,
	c: db.PoolClient,
	f: Pick<Fact, "factId" | "revision" | "tenantId">,
	target: number,
	action: ProjectionAction,
	epoch: number,
): Promise<void> {
	const taskId = projectionTaskIdOf(f.factId, target);
	await mdb.upsertProjection(c, { factId: f.factId, target, factRevision: f.revision, action, epoch, taskId });
	const input: ProjectionInput = {
		schemaVersion: 1,
		computation: projectionProfile,
		factId: f.factId,
		factRevision: f.revision,
		target,
		action,
		epoch,
	};
	await tasks.requestIn(c, {
		taskId,
		tenantId: f.tenantId,
		kind: "memory-project",
		profile: projectionProfile,
		input: JSON.stringify(input),
		effects: "reconstructible",
		dispatchId: "",
		authorizationRef: `memory:${f.factId}`,
		correlationId: `${f.factId}-r${f.revision}`,
	});
}
