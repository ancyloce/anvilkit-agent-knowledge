// The memory projections (DD-07 §5): the memory-project task that applies a
// fact's current state to one target — the PostgresStore (target 0) or the
// Qdrant collection of one index generation (target g) — and the
// reconciliation that backfills new generations and retries failed rows.
//
// The rule that keeps a stale write from reviving a revoked or deleted fact:
// a projection never writes what its task carried, it writes what the
// authoritative fact says now (content when confirmed and undeleted, a
// content-free tombstone or no point otherwise), verifies the write, and
// reads the fact again; if the fact changed meanwhile it writes the new
// state and repeats. The last write to any target therefore always follows
// the last committed decision. Recall never trusts a projection: it
// discloses only authoritative content after its own recheck.
// No Store, Qdrant or Inference I/O runs while a row is locked.
import * as idx from "../adapters/indexdb.js";
import { InferenceError, InferenceMismatch, type InferencePort } from "../adapters/inference.js";
import * as mdb from "../adapters/memorydb.js";
import { type MemoryStorePort, type StoredFact, StoreUnavailable } from "../adapters/memorystore.js";
import * as db from "../adapters/postgres.js";
import { type VectorIndex, VectorUnavailable } from "../adapters/qdrant.js";
import { type IndexGeneration, type SpaceProfile, sameSpace } from "../domain/index.js";
import {
	type Fact,
	MemoryError,
	type MemoryPayload,
	memoryKeyOf,
	memoryPointIdOf,
	type ProjectionAction,
	parseProjectionInput,
	projectionDigest,
	projectionResultRefOf,
	storeTarget,
} from "../domain/memory.js";
import type { Submission, Task } from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import { type MemoryBounds, requestProjectionIn } from "./memory.js";
import type { Clock, PreparedResult, ResultRecords, Tasks } from "./tasks.js";

export type ProjectionAnswer =
	| { state: "running"; retryAfterMs: number }
	| { state: "materialized"; resultRef: string; resultDigest: string }
	| { state: "failed"; failureCode: string };

export const projectionFailure = {
	superseded: "SUPERSEDED",
	storeUnavailable: "STORE_UNAVAILABLE",
	vectorsUnavailable: "INDEX_UNAVAILABLE",
	computeRefused: "COMPUTE_REFUSED",
	profileUnqualified: "PROFILE_UNQUALIFIED",
	factMissing: "FACT_MISSING",
} as const;

/** One verified application of a fact's state to a target. */
interface Applied {
	revision: number;
	action: ProjectionAction;
	contentDigest: string;
	modelRevision: string;
}

type Step = { ok: true; applied: Applied } | { ok: false; retry: true } | { ok: false; retry: false; code: string };

/** The rounds a projection re-reads a fact that keeps changing before it answers running. */
const convergeRounds = 4;
const retryMs = 500;

export class MemoryProjector implements ResultRecords {
	private open: () => boolean = () => true;

	constructor(
		private readonly store: db.Store,
		private readonly tasks: Tasks,
		private readonly memoryStore: () => MemoryStorePort | undefined,
		private readonly vectors: () => VectorIndex | undefined,
		private readonly inference: () => InferencePort | undefined,
		private readonly space: () => SpaceProfile | undefined,
		private readonly bounds: () => MemoryBounds,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	// ---------------------------------------------------------------------
	// AdvanceProjection
	// ---------------------------------------------------------------------

	private async claimed(taskId: string, generation: number, workerId: string, inputDigest: string): Promise<Task> {
		const task = await db.getRequest(this.store.pool, taskId, generation);
		if (!task) throw new MemoryError("NOT_FOUND", `${taskId} generation ${generation}`);
		if (task.kind !== "memory-project") throw new MemoryError("INVALID_ARGUMENT", "not a memory-project task");
		if (task.inputDigest !== inputDigest)
			throw new MemoryError("INVALID_ARGUMENT", "input digest differs from the frozen input");
		const now = this.clock.now().getTime();
		if (task.state !== "leased" || task.workerId !== workerId || !task.leaseUntil || task.leaseUntil.getTime() <= now)
			throw new MemoryError("STALE_EXECUTION", `not the current claimant (state ${task.state})`);
		return task;
	}

	/**
	 * P23: while the removal inventory is not reconciled no projection is
	 * written, so a restored request never applies content a restore revived.
	 */
	setGate(open: () => boolean): void {
		this.open = open;
	}

	async advance(taskId: string, generation: number, workerId: string, inputDigest: string): Promise<ProjectionAnswer> {
		if (!this.open()) return { state: "running", retryAfterMs: 1000 };
		const task = await this.claimed(taskId, generation, workerId, inputDigest);
		const input = parseProjectionInput(task.input);
		const at = { factRevision: input.factRevision, epoch: input.epoch };
		const row = await mdb.projectionByTask(this.store.pool, taskId);
		if (!row || row.factRevision !== input.factRevision || row.epoch !== input.epoch)
			return { state: "failed", failureCode: projectionFailure.superseded };
		if (row.state === "materialized" || row.state === "accepted")
			return {
				state: "materialized",
				resultRef: projectionResultRefOf(row.factId, row.factRevision, row.target),
				resultDigest: row.manifestDigest,
			};
		if (row.state === "failed" || row.state === "stale")
			return { state: "failed", failureCode: row.failureCode || projectionFailure.superseded };
		if (row.state === "pending") await mdb.updateProjection(this.store.pool, taskId, at, { state: "running" });
		const kind = input.target === storeTarget ? "store" : "vectors";
		const step = await this.applyCurrent(input.factId, input.target, taskId);
		if (!step.ok) {
			if (step.retry) {
				this.metrics.memoryProjections.inc({ target: kind, outcome: "retry" });
				return { state: "running", retryAfterMs: retryMs };
			}
			await mdb.updateProjection(this.store.pool, taskId, at, { state: "failed", failureCode: step.code });
			this.metrics.memoryProjections.inc({ target: kind, outcome: "failed" });
			return { state: "failed", failureCode: step.code };
		}
		// The target now holds a newer revision than this task carries: the
		// newer task records it; this one is superseded.
		if (step.applied.revision !== input.factRevision) {
			this.metrics.memoryProjections.inc({ target: kind, outcome: "superseded" });
			return { state: "failed", failureCode: projectionFailure.superseded };
		}
		const digest = projectionDigest({ factId: input.factId, target: input.target, ...step.applied });
		if (
			(await mdb.updateProjection(this.store.pool, taskId, at, { state: "materialized", manifestDigest: digest })) !== 1
		)
			return { state: "failed", failureCode: projectionFailure.superseded };
		this.metrics.memoryProjections.inc({
			target: kind,
			outcome: step.applied.action === "apply" ? "applied" : "removed",
		});
		return {
			state: "materialized",
			resultRef: projectionResultRefOf(input.factId, input.factRevision, input.target),
			resultDigest: digest,
		};
	}

	/** Writes the fact's current state to the target, verifies it, and repeats while the fact changes. */
	private async applyCurrent(factId: string, target: number, taskId: string): Promise<Step> {
		for (let round = 0; round < convergeRounds; round++) {
			const f = await mdb.getFact(this.store.pool, factId);
			if (!f) return { ok: false, retry: false, code: projectionFailure.factMissing };
			let step: Step;
			try {
				step = target === storeTarget ? await this.writeStore(f) : await this.writeVectors(f, target, taskId);
			} catch (err) {
				if (err instanceof StoreUnavailable || err instanceof VectorUnavailable) {
					this.log.warn("memory projection not confirmed; written again", { factId, target, error: err.message });
					return { ok: false, retry: true };
				}
				throw err;
			}
			if (!step.ok) return step;
			const again = await mdb.getFact(this.store.pool, factId);
			if (again && again.revision === f.revision) return step;
		}
		return { ok: false, retry: true };
	}

	private async writeStore(f: Fact): Promise<Step> {
		const ms = this.memoryStore();
		if (!ms) return { ok: false, retry: false, code: projectionFailure.storeUnavailable };
		const apply = f.state === "confirmed" && !f.deleted;
		const existing = await ms.get(f.tenantId, f.factId);
		const value: StoredFact = apply
			? {
					factId: f.factId,
					revision: f.revision,
					state: f.state,
					tombstone: false,
					contentDigest: f.contentDigest,
					content: f.content,
					subjectType: f.subjectType,
					subjectId: f.subjectId,
					scopeId: f.scopeId,
					sourceRefs: f.sourceRefs,
					...(f.expiresAt ? { expiresAt: f.expiresAt.toISOString() } : {}),
				}
			: {
					factId: f.factId,
					revision: f.revision,
					state: f.deleted ? "deleted" : f.state,
					tombstone: true,
					contentDigest: "",
				};
		// Never overwrite a newer projection with an older state.
		if (!existing || existing.revision <= f.revision) await ms.put(f.tenantId, value);
		const stored = await ms.get(f.tenantId, f.factId);
		const verified =
			stored !== undefined &&
			stored.revision === f.revision &&
			stored.tombstone === !apply &&
			(apply ? stored.content === f.content && stored.contentDigest === f.contentDigest : stored.content === undefined);
		if (!verified) return { ok: false, retry: true };
		return {
			ok: true,
			applied: {
				revision: f.revision,
				action: apply ? "apply" : "remove",
				contentDigest: f.contentDigest,
				modelRevision: "",
			},
		};
	}

	private async writeVectors(f: Fact, target: number, taskId: string): Promise<Step> {
		const vectors = this.vectors();
		if (!vectors) return { ok: false, retry: false, code: projectionFailure.vectorsUnavailable };
		const g = await idx.getGeneration(this.store.pool, target);
		const space = this.space();
		const pointId = memoryPointIdOf(f.factId);
		const exists = g ? await vectors.exists(g.collectionName) : false;
		// Content only into an open generation of the reviewed space; a retired
		// or foreign generation only ever loses the point.
		const apply =
			f.state === "confirmed" && !f.deleted && g?.state !== "retired" && !!space && !!g && sameSpace(g, space);
		if (!apply) {
			if (g && exists) {
				await vectors.deleteIds(g.collectionName, [pointId]);
				const left = await vectors.retrieve(g.collectionName, [pointId]);
				if (left.has(pointId)) return { ok: false, retry: true };
			}
			return {
				ok: true,
				applied: {
					revision: f.revision,
					action: "remove",
					contentDigest: f.contentDigest,
					modelRevision: g?.modelRevision ?? "",
				},
			};
		}
		// The collection is created by the Index Builder's reconciliation.
		if (!exists) return { ok: false, retry: true };
		const inference = this.inference();
		if (!inference) return { ok: false, retry: false, code: projectionFailure.vectorsUnavailable };
		let embedded: Awaited<ReturnType<InferencePort["embed"]>>;
		try {
			embedded = await inference.embed({ taskId, generation: String(target) }, "passage", [f.content]);
		} catch (err) {
			if (err instanceof InferenceError && err.retryable) return { ok: false, retry: true };
			if (err instanceof InferenceMismatch)
				return { ok: false, retry: false, code: projectionFailure.profileUnqualified };
			if (err instanceof InferenceError) return { ok: false, retry: false, code: projectionFailure.computeRefused };
			throw err;
		}
		const gen = g as IndexGeneration;
		const payload = payloadOf(f, gen.generation);
		await vectors.upsert(gen.collectionName, [
			{
				id: pointId,
				dense: embedded.dense[0] as number[],
				sparse: embedded.sparse[0] as { indices: number[]; values: number[] },
				payload: payload as never,
			},
		]);
		const stored = (await vectors.retrieve(gen.collectionName, [pointId])).get(pointId);
		const p = stored?.payload ?? {};
		if (!stored?.hasDense || !stored.hasSparse || Object.entries(payload).some(([k, v]) => p[k] !== v))
			return { ok: false, retry: true };
		return {
			ok: true,
			applied: {
				revision: f.revision,
				action: "apply",
				contentDigest: f.contentDigest,
				modelRevision: gen.modelRevision,
			},
		};
	}

	// ---------------------------------------------------------------------
	// ResultRecords of memory-project-v1
	// ---------------------------------------------------------------------

	async prepare(task: Task, _sub: Submission): Promise<PreparedResult | undefined> {
		const input = parseProjectionInput(task.input);
		const row = await mdb.projectionByTask(this.store.pool, task.taskId);
		if (
			row?.state !== "materialized" ||
			row.factRevision !== input.factRevision ||
			row.epoch !== input.epoch ||
			!row.manifestDigest
		)
			return undefined;
		// The recorded application still holds: the fact is at that revision.
		const f = await mdb.getFact(this.store.pool, row.factId);
		if (!f || f.revision !== row.factRevision) return undefined;
		return {
			expected: { ref: projectionResultRefOf(row.factId, row.factRevision, row.target), digest: row.manifestDigest },
			payload: row,
		};
	}

	async onAccepted(c: db.PoolClient, task: Task, prepared: PreparedResult): Promise<void> {
		const recorded = prepared.payload as mdb.Projection;
		const row = await mdb.projectionByTask(c, task.taskId, true);
		if (!row || row.manifestDigest !== recorded.manifestDigest || row.factRevision !== recorded.factRevision)
			throw new MemoryError("STALE_EXECUTION", "the projection changed after its preparation");
		await mdb.updateProjection(c, task.taskId, row, { state: "accepted" });
		if (await mdb.markPurgedIfComplete(c, row.factId))
			this.log.info("deleted memory fact purged from every projection", { factId: row.factId });
	}

	async onEnded(c: db.PoolClient, task: Task): Promise<void> {
		const input = parseProjectionInput(task.input);
		const row = await mdb.projectionByTask(c, task.taskId, true);
		if (!row || row.state === "accepted" || row.factRevision !== input.factRevision || row.epoch !== input.epoch)
			return;
		await mdb.updateProjection(c, task.taskId, row, {
			state: task.state === "stale" || task.state === "canceled" ? "stale" : "failed",
			failureCode: task.failureCode || task.state.toUpperCase(),
		});
	}

	// ---------------------------------------------------------------------
	// Reconciliation: backfill and retry (P17-04); expiry and rebuild (P17-06)
	// ---------------------------------------------------------------------

	/** The open generations of the reviewed space whose collections exist (memory points go there). */
	private async openGenerations(): Promise<IndexGeneration[]> {
		const space = this.space();
		const vectors = this.vectors();
		if (!space || !vectors) return [];
		const out: IndexGeneration[] = [];
		for (const g of await idx.listGenerations(this.store.pool))
			if (g.state !== "retired" && sameSpace(g, space) && (await vectors.exists(g.collectionName))) out.push(g);
		return out;
	}

	async reconcile(): Promise<void> {
		const targets = [storeTarget, ...(await this.openGenerations()).map((g) => g.generation)];
		for (const target of targets) await this.backfill(target);
		await this.retryFailed();
	}

	/** Requests every fact a target lacks at its current revision (a new generation, a lost Store row). */
	private async backfill(target: number): Promise<void> {
		for (;;) {
			const missing = await mdb.missingFor(this.store.pool, target, 100);
			if (missing.length === 0) return;
			let requested = 0;
			for (const m of missing)
				requested += await this.store.inTx(async (c) => {
					const f = await mdb.getFact(c, m.factId, true);
					if (!f || f.revision !== m.revision) return 0;
					const rows = await mdb.projectionsOf(c, f.factId, true);
					const row = rows.find((r) => r.target === target);
					if (row?.factRevision === f.revision) return 0;
					await requestProjectionIn(
						this.tasks,
						c,
						f,
						target,
						f.state === "confirmed" && !f.deleted ? "apply" : "remove",
						row?.epoch ?? 0,
					);
					return 1;
				});
			if (requested === 0) return;
		}
	}

	/** A failed or fenced row of a fact's current revision gets a new epoch after the retry delay, within the bound. */
	private async retryFailed(): Promise<void> {
		const b = this.bounds();
		// Database time: the rows' updated_at is written by the database.
		const rows = await mdb.failedProjections(this.store.pool, b.retryDelayMs, b.maxProjectionEpochs, 50);
		for (const r of rows)
			await this.store.inTx(async (c) => {
				const f = await mdb.getFact(c, r.factId, true);
				if (!f || f.revision !== r.factRevision) return;
				const now = (await mdb.projectionsOf(c, r.factId, true)).find((x) => x.target === r.target);
				if (!now || (now.state !== "failed" && now.state !== "stale") || now.epoch !== r.epoch) return;
				await requestProjectionIn(this.tasks, c, f, r.target, r.action, r.epoch + 1);
			});
	}

	/**
	 * Recovery: rebuilds one target (the Store after its schema was lost or
	 * restored, or a vector generation) from the authoritative facts. Every
	 * fact the target ever held or should hold is requested again under a new
	 * epoch, carrying the fact's current revision: confirmed facts are
	 * applied, revoked, expired and deleted facts become tombstones again,
	 * so a rebuild never revives what a decision removed. The Worker then
	 * runs the requests like any other projection.
	 */
	async rebuild(target: number): Promise<number> {
		let after = "";
		let requested = 0;
		for (;;) {
			const r = await this.store.pool.query<{ fact_id: string }>(
				`SELECT f.fact_id FROM memory_facts f
				 WHERE f.fact_id > $2 AND ((f.state = 'confirmed' AND NOT f.deleted)
				   OR EXISTS (SELECT 1 FROM memory_projections p WHERE p.fact_id = f.fact_id AND ($1 = 0 OR p.target = $1)))
				 ORDER BY f.fact_id LIMIT 100`,
				[target, after],
			);
			if (r.rows.length === 0) return requested;
			for (const { fact_id } of r.rows)
				requested += await this.store.inTx(async (c) => {
					const f = await mdb.getFact(c, fact_id, true);
					if (!f) return 0;
					const row = (await mdb.projectionsOf(c, f.factId, true)).find((x) => x.target === target);
					await requestProjectionIn(
						this.tasks,
						c,
						f,
						target,
						f.state === "confirmed" && !f.deleted ? "apply" : "remove",
						row ? row.epoch + 1 : 0,
					);
					return 1;
				});
			after = r.rows[r.rows.length - 1]?.fact_id ?? after;
		}
	}

	/**
	 * Whether a building or materialized generation holds exactly the memory
	 * points it should: every live confirmed fact accepted there at its
	 * current revision, no open row, and the collection's memory point count
	 * equal to that (the Index Builder's qualification asks this).
	 */
	async generationReady(
		g: IndexGeneration,
		vectors: VectorIndex,
	): Promise<{ ok: true } | { ok: false; reason: string }> {
		const p = await mdb.memoryProgress(this.store.pool, g.generation);
		if (p.open > 0) return { ok: false, reason: `${p.open} memory projections open` };
		if (p.accepted !== p.live) return { ok: false, reason: `${p.accepted} of ${p.live} confirmed facts projected` };
		const counted = await vectors.count(g.collectionName, { must: [{ key: "kind", match: { value: "memory" } }] });
		if (counted !== p.live) return { ok: false, reason: `${counted} memory points counted, ${p.live} confirmed facts` };
		return { ok: true };
	}
}

function payloadOf(f: Fact, generation: number): MemoryPayload {
	return {
		kind: "memory",
		tenant_id: f.tenantId,
		scope_id: f.scopeId,
		subject_type: f.subjectType,
		subject_id: f.subjectId,
		fact_id: f.factId,
		fact_revision: f.revision,
		memory_key: memoryKeyOf(f.factId, f.revision),
		content_digest: f.contentDigest,
		generation,
	};
}
