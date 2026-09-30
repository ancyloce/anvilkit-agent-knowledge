// The Index Builder (DD-07 §3/§6): index generations as Qdrant collections,
// the knowledge-project task that writes one source revision into one
// generation, and the reconciliation that creates, fills, qualifies and
// accepts generations.
//
// AdvanceIndex serves only the current claimant. Each call embeds one
// bounded batch of accepted chunks (text read from the verified parser
// result, never from Qdrant) through Inference and upserts it under the
// chunks' deterministic point ids, waiting for processing; the entry's
// watermark moves only after the write is confirmed, so an unanswered or
// partial upsert is simply written again under the same ids. When every
// chunk is written, Knowledge reads every point back, compares identities,
// digests and both named vectors, counts the source revision's points
// exactly (stray points are deleted) and records the manifest digest: the
// only result acceptance will take. A generation is accepted (the stable
// alias moves to it) only after every eligible source revision has an
// accepted entry and the collection holds exactly the ledger's points.
// No Qdrant, Inference or storage I/O runs while a row is locked.

import * as idx from "../adapters/indexdb.js";
import type { InferencePort } from "../adapters/inference.js";
import { InferenceError, InferenceMismatch } from "../adapters/inference.js";
import * as idb from "../adapters/ingestdb.js";
import type { ObjectStore } from "../adapters/objects.js";
import * as db from "../adapters/postgres.js";
import { type Filter, type Point, type VectorIndex, VectorUnavailable } from "../adapters/qdrant.js";
import { sourceIndexedEvent } from "../domain/event.js";
import {
	collectionOf,
	type IndexEntry,
	IndexError,
	type IndexGeneration,
	type IndexInput,
	indexFailure,
	indexProfile,
	indexResultRefOf,
	indexTaskIdOf,
	manifestDigest,
	parseIndexInput,
	pointIdOf,
	pointMismatch,
	qualifies,
	type SpaceProfile,
	sameSpace,
	sourceKeyOf,
} from "../domain/index.js";
import type { Submission, Task } from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import { ChunkTexts, ChunkUnverified } from "./chunktext.js";
import type { Clock, PreparedResult, ResultRecords, Tasks } from "./tasks.js";

export type IndexAnswer =
	| { state: "running"; retryAfterMs: number }
	| { state: "materialized"; resultRef: string; resultDigest: string }
	| { state: "failed"; failureCode: string };

export interface IndexSettings {
	batchSize: number;
	maxResultBytes: number;
	pollMs: number;
}

/** The parsed revision whose accepted chunks become index entries. */
export interface Indexable {
	requestId: string;
	sourceId: string;
	sourceRevision: number;
	tenantId: string;
	chunkerProfile: string;
	chunkerRevision: number;
	chunkCount: number;
	correlationId: string;
}

const verifyPage = 256;

export class Indexer implements ResultRecords {
	constructor(
		private readonly store: db.Store,
		private readonly tasks: Tasks,
		private readonly vectors: () => VectorIndex | undefined,
		private readonly inference: () => InferencePort | undefined,
		private readonly objects: () => ObjectStore | undefined,
		private readonly space: () => SpaceProfile | undefined,
		private readonly settings: () => IndexSettings,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	// ---------------------------------------------------------------------
	// Scheduling
	// ---------------------------------------------------------------------

	/**
	 * Inside the transaction that accepted a parse: one entry and one
	 * knowledge-project request per open generation built from the same
	 * chunker. Nothing is written to Qdrant here.
	 */
	async scheduleIn(c: db.PoolClient, r: Indexable): Promise<number> {
		let n = 0;
		for (const g of await idx.listGenerations(c)) {
			if (g.state === "retired" || g.chunkerProfile !== r.chunkerProfile || g.chunkerRevision !== r.chunkerRevision)
				continue;
			if (await this.entryIn(c, g, r)) n++;
		}
		return n;
	}

	private async entryIn(c: db.PoolClient, g: IndexGeneration, r: Indexable): Promise<boolean> {
		const taskId = indexTaskIdOf(r.sourceId, r.sourceRevision, g.generation);
		const inserted = await idx.insertEntry(c, {
			generation: g.generation,
			sourceId: r.sourceId,
			sourceRevision: r.sourceRevision,
			ingestRequestId: r.requestId,
			taskId,
			chunkCount: r.chunkCount,
		});
		if (!inserted) return false;
		const input: IndexInput = {
			schemaVersion: 1,
			computation: indexProfile,
			sourceId: r.sourceId,
			sourceRevision: r.sourceRevision,
			ingestRequestId: r.requestId,
			indexGeneration: g.generation,
			chunkCount: r.chunkCount,
			embeddingProfile: g.embeddingProfile,
			modelRevision: g.modelRevision,
		};
		await this.tasks.requestIn(c, {
			taskId,
			tenantId: r.tenantId,
			kind: "knowledge-project",
			profile: indexProfile,
			input: JSON.stringify(input),
			effects: "reconstructible",
			dispatchId: "",
			authorizationRef: `source:${r.sourceId}`,
			correlationId: r.correlationId,
		});
		return true;
	}

	// ---------------------------------------------------------------------
	// AdvanceIndex
	// ---------------------------------------------------------------------

	private async claimed(taskId: string, generation: number, workerId: string, inputDigest: string): Promise<Task> {
		const task = await db.getRequest(this.store.pool, taskId, generation);
		if (!task) throw new IndexError("NOT_FOUND", `${taskId} generation ${generation}`);
		if (task.kind !== "knowledge-project") throw new IndexError("INVALID_ARGUMENT", "not a knowledge-project task");
		if (task.inputDigest !== inputDigest)
			throw new IndexError("INVALID_ARGUMENT", "input digest differs from the frozen input");
		const now = this.clock.now().getTime();
		if (task.state !== "leased" || task.workerId !== workerId || !task.leaseUntil || task.leaseUntil.getTime() <= now)
			throw new IndexError("STALE_EXECUTION", `not the current claimant (state ${task.state})`);
		return task;
	}

	/** Whether the generation's space is the one the reviewed Inference profile computes. */
	private computes(g: IndexGeneration): boolean {
		const s = this.space();
		return s !== undefined && sameSpace(g, s);
	}

	private async fail(entry: IndexEntry, code: string): Promise<IndexAnswer> {
		await idx.updateEntry(this.store.pool, entry.taskId, { state: "failed", failureCode: code });
		this.metrics.indexBatches.inc({ outcome: "failed" });
		return { state: "failed", failureCode: code };
	}

	private retry(outcome: string): IndexAnswer {
		this.metrics.indexBatches.inc({ outcome });
		return { state: "running", retryAfterMs: Math.max(this.settings().pollMs, 200) };
	}

	async advance(taskId: string, generation: number, workerId: string, inputDigest: string): Promise<IndexAnswer> {
		const task = await this.claimed(taskId, generation, workerId, inputDigest);
		const input = parseIndexInput(task.input);
		const entry = await idx.getEntryByTask(this.store.pool, taskId);
		if (!entry) throw new IndexError("NOT_FOUND", `index entry of ${taskId}`);
		if (entry.state === "materialized" || entry.state === "accepted")
			return {
				state: "materialized",
				resultRef: indexResultRefOf(entry.generation, entry.sourceId, entry.sourceRevision),
				resultDigest: entry.manifestDigest,
			};
		if (entry.state === "failed" || entry.state === "stale")
			return { state: "failed", failureCode: entry.failureCode || indexFailure.unavailable };
		const vectors = this.vectors();
		const inference = this.inference();
		const objects = this.objects();
		if (!vectors || !inference || !objects) return { state: "failed", failureCode: indexFailure.unavailable };
		const g = await idx.getGeneration(this.store.pool, input.indexGeneration);
		if (!g || g.state === "retired") return this.fail(entry, indexFailure.generationRetired);
		if (!this.computes(g) || g.modelRevision !== input.modelRevision || g.embeddingProfile !== input.embeddingProfile)
			return this.fail(entry, indexFailure.profileUnqualified);
		const source = await idb.getSource(this.store.pool, input.sourceId);
		if (!source || source.deleted) return this.fail(entry, indexFailure.authorizationRevoked);
		if (entry.state === "pending") await idx.updateEntry(this.store.pool, taskId, { state: "running" });
		const texts = new ChunkTexts(objects, this.settings().maxResultBytes);
		const key = sourceKeyOf(entry.sourceId, entry.sourceRevision);
		const payloadOf = (ch: idb.ChunkRow) => ({
			tenant_id: source.tenantId,
			project_id: source.projectId,
			source_id: ch.sourceId,
			source_revision: ch.sourceRevision,
			source_key: key,
			chunk_id: ch.chunkId,
			ordinal: ch.ordinal,
			content_digest: ch.contentDigest,
			generation: g.generation,
		});

		if (entry.writtenThrough < entry.chunkCount) {
			const rows = await idx.chunkPage(
				this.store.pool,
				entry.ingestRequestId,
				entry.writtenThrough,
				this.settings().batchSize,
			);
			if (rows.length === 0 || rows[0]?.ordinal !== entry.writtenThrough)
				return this.fail(entry, indexFailure.chunkUnverified);
			let batch: string[];
			try {
				batch = await Promise.all(rows.map((r) => texts.text(r)));
			} catch (err) {
				if (err instanceof ChunkUnverified) {
					this.log.warn("chunk text unverified", { taskId, reason: err.message });
					return this.fail(entry, indexFailure.chunkUnverified);
				}
				throw err;
			}
			let embedded: Awaited<ReturnType<InferencePort["embed"]>>;
			try {
				embedded = await inference.embed({ taskId, generation: String(generation) }, "passage", batch);
			} catch (err) {
				if (err instanceof InferenceError && err.retryable) return this.retry("retry");
				if (err instanceof InferenceMismatch) return this.fail(entry, indexFailure.profileUnqualified);
				if (err instanceof InferenceError) return this.fail(entry, indexFailure.computeRefused);
				throw err;
			}
			const points: Point[] = rows.map((r, i) => ({
				id: pointIdOf(r.chunkId),
				dense: embedded.dense[i] as number[],
				sparse: embedded.sparse[i] as { indices: number[]; values: number[] },
				payload: payloadOf(r),
			}));
			try {
				await vectors.upsert(g.collectionName, points);
			} catch (err) {
				// Unanswered or refused: the watermark stays, the same ids are written again.
				if (err instanceof VectorUnavailable) {
					this.log.warn("index batch not confirmed; written again", { taskId, error: err.message });
					return this.retry("retry");
				}
				throw err;
			}
			const last = rows[rows.length - 1] as idb.ChunkRow;
			await idx.updateEntry(this.store.pool, taskId, { writtenThrough: last.ordinal + 1 }, entry.writtenThrough);
			this.metrics.indexBatches.inc({ outcome: "written" });
			return { state: "running", retryAfterMs: 0 };
		}

		// Every chunk was written: read the points back before anything is recorded.
		const manifest: { pointId: string; chunkId: string; contentDigest: string }[] = [];
		try {
			for (let from = 0; from < entry.chunkCount; from += verifyPage) {
				const rows = await idx.chunkPage(this.store.pool, entry.ingestRequestId, from, verifyPage);
				const stored = await vectors.retrieve(
					g.collectionName,
					rows.map((r) => pointIdOf(r.chunkId)),
				);
				for (const r of rows) {
					const id = pointIdOf(r.chunkId);
					const why = pointMismatch(stored.get(id), payloadOf(r));
					if (why) {
						// Rewrite from the first point that does not prove its chunk.
						this.log.warn("index point not materialized; rewriting", { taskId, ordinal: r.ordinal, reason: why });
						await idx.updateEntry(this.store.pool, taskId, { writtenThrough: r.ordinal }, entry.writtenThrough);
						return this.retry("rewrite");
					}
					manifest.push({ pointId: id, chunkId: r.chunkId, contentDigest: r.contentDigest });
				}
			}
			const bySource: Filter = { must: [{ key: "source_key", match: { value: key } }] };
			const counted = await vectors.count(g.collectionName, bySource);
			if (counted !== entry.chunkCount) {
				// Points of this revision that no chunk names are removed.
				await vectors.deleteWhere(g.collectionName, {
					must: bySource.must,
					must_not: [{ has_id: manifest.map((m) => m.pointId) }],
				});
				return this.retry("rewrite");
			}
		} catch (err) {
			if (err instanceof VectorUnavailable) return this.retry("retry");
			throw err;
		}
		const digest = manifestDigest(g, key, manifest);
		await idx.updateEntry(this.store.pool, taskId, {
			state: "materialized",
			pointCount: manifest.length,
			manifestDigest: digest,
		});
		this.metrics.indexBatches.inc({ outcome: "materialized" });
		return {
			state: "materialized",
			resultRef: indexResultRefOf(entry.generation, entry.sourceId, entry.sourceRevision),
			resultDigest: digest,
		};
	}

	// ---------------------------------------------------------------------
	// ResultRecords of knowledge-index-v1
	// ---------------------------------------------------------------------

	async prepare(task: Task, _sub: Submission): Promise<PreparedResult | undefined> {
		const entry = await idx.getEntryByTask(this.store.pool, task.taskId);
		if (entry?.state !== "materialized" || !entry.manifestDigest) return undefined;
		const g = await idx.getGeneration(this.store.pool, entry.generation);
		const vectors = this.vectors();
		if (!g || g.state === "retired" || !vectors) return undefined;
		// The recorded manifest still describes the collection: an exact count of the revision's points.
		try {
			const n = await vectors.count(g.collectionName, {
				must: [{ key: "source_key", match: { value: sourceKeyOf(entry.sourceId, entry.sourceRevision) } }],
			});
			if (n !== entry.pointCount) return undefined;
		} catch (err) {
			if (err instanceof VectorUnavailable) return undefined;
			throw err;
		}
		return {
			expected: {
				ref: indexResultRefOf(entry.generation, entry.sourceId, entry.sourceRevision),
				digest: entry.manifestDigest,
			},
			payload: entry,
		};
	}

	async onAccepted(c: db.PoolClient, task: Task, prepared: PreparedResult): Promise<void> {
		const recorded = prepared.payload as IndexEntry;
		const entry = await idx.getEntryByTask(c, task.taskId, true);
		if (!entry || entry.manifestDigest !== recorded.manifestDigest)
			throw new IndexError("STALE_EXECUTION", "the index entry changed after its preparation");
		await idx.updateEntry(c, task.taskId, { state: "accepted" });
		const g = await idx.getGeneration(c, entry.generation);
		if (g?.state === "accepted") await this.markIndexed(c, entry, g.generation, task.tenantId, task.correlationId);
	}

	/** The ingest becomes indexed once its revision is readable through the accepted generation. */
	private async markIndexed(
		c: db.PoolClient,
		entry: Pick<IndexEntry, "ingestRequestId" | "sourceId" | "sourceRevision">,
		generation: number,
		tenantId: string,
		correlationId: string,
	): Promise<void> {
		const r = await c.query(
			"UPDATE ingest_requests SET state = 'indexed', updated_at = now() WHERE request_id = $1 AND state = 'indexing'",
			[entry.ingestRequestId],
		);
		if ((r.rowCount ?? 0) === 1)
			await db.publishOutbox(
				c,
				sourceIndexedEvent(
					{ sourceId: entry.sourceId, tenantId, sourceRevision: entry.sourceRevision, indexGeneration: generation },
					correlationId,
					this.clock.now(),
				),
			);
	}

	async onEnded(c: db.PoolClient, task: Task): Promise<void> {
		const entry = await idx.getEntryByTask(c, task.taskId, true);
		if (!entry || entry.state === "accepted" || entry.state === "stale") return;
		await idx.updateEntry(c, task.taskId, {
			state: task.state === "stale" || task.state === "canceled" ? "stale" : "failed",
			failureCode: task.failureCode || task.state.toUpperCase(),
		});
	}

	// ---------------------------------------------------------------------
	// Reconciliation: generations, collections, backfill, qualification,
	// alias and the deletion purge (P16-05).
	// ---------------------------------------------------------------------

	private ensured = new Set<string>();

	/**
	 * P17: the memory points a generation must also hold before it
	 * qualifies (MemoryProjector.generationReady); without a gate only the
	 * document points count.
	 */
	private memoryGate?: (
		g: IndexGeneration,
		vectors: VectorIndex,
	) => Promise<{ ok: true } | { ok: false; reason: string }>;
	setMemoryGate(gate: NonNullable<Indexer["memoryGate"]>): void {
		this.memoryGate = gate;
	}

	/** Records a new building generation for the active space (a rebuild when one already serves it). */
	async createGeneration(): Promise<number | undefined> {
		const space = this.space();
		if (!space) return undefined;
		try {
			return await this.store.inTx((c) => idx.insertGeneration(c, space, collectionOf));
		} catch (err) {
			if ((err as { code?: string }).code === "23505") return undefined; // another replica created it
			throw err;
		}
	}

	async reconcile(): Promise<void> {
		const vectors = this.vectors();
		const space = this.space();
		if (!vectors || !space) return;
		let gens = await idx.listGenerations(this.store.pool);
		const serving = gens.find(
			(g) =>
				g.state !== "retired" &&
				sameSpace(g, space) &&
				g.chunkerProfile === space.chunkerProfile &&
				g.chunkerRevision === space.chunkerRevision,
		);
		if (!serving) {
			const n = await this.createGeneration();
			if (n) this.log.info("index generation created", { generation: n, modelRevision: space.modelRevision });
			gens = await idx.listGenerations(this.store.pool);
		}
		for (const g of gens) {
			if (g.state === "retired" || this.ensured.has(g.collectionName)) continue;
			await vectors.ensureCollection(g);
			this.ensured.add(g.collectionName);
		}
		await this.purge(vectors, gens);
		for (const g of gens.filter((x) => x.state === "building" || x.state === "materialized")) {
			await this.backfill(g);
			await this.qualify(vectors, g);
		}
		gens = await idx.listGenerations(this.store.pool);
		const accepted = gens.find((g) => g.state === "accepted");
		if (accepted) await vectors.switchAlias(accepted.collectionName);
		for (const s of ["building", "materialized", "accepted", "retired"] as const)
			this.metrics.indexGeneration.set(
				{ state: s },
				Math.max(0, ...gens.filter((g) => g.state === s).map((g) => g.generation)),
			);
	}

	/** Every eligible revision of a generation that has no entry yet gets one (idempotent). */
	private async backfill(g: IndexGeneration): Promise<void> {
		let after = "";
		for (;;) {
			const page = await idx.eligibleRevisions(this.store.pool, g.chunkerProfile, g.chunkerRevision, after, 100);
			if (page.length === 0) return;
			await this.store.inTx(async (c) => {
				for (const r of page)
					await this.entryIn(c, g, {
						requestId: r.ingestRequestId,
						sourceId: r.sourceId,
						sourceRevision: r.sourceRevision,
						tenantId: r.tenantId,
						chunkerProfile: g.chunkerProfile,
						chunkerRevision: g.chunkerRevision,
						chunkCount: r.chunkCount,
						correlationId: `index-generation-${g.generation}`,
					});
			});
			after = page[page.length - 1]?.ingestRequestId ?? after;
		}
	}

	/**
	 * building -> materialized when every eligible revision is accepted and
	 * the collection's exact point count equals the ledger; materialized ->
	 * accepted after the same check again, with the previous accepted
	 * generation retired in the same transaction. A materialized generation
	 * that no longer qualifies (a newer source arrived) returns to building.
	 */
	private async qualify(vectors: VectorIndex, g: IndexGeneration): Promise<void> {
		const p = await idx.progressOf(this.store.pool, g);
		// Document points only: memory points (P17) are counted by the gate.
		const counted = await vectors.count(g.collectionName, {
			must: [],
			must_not: [{ key: "kind", match: { value: "memory" } }],
		});
		let q = qualifies({ ...p, countedPoints: counted });
		if (q.ok && this.memoryGate) q = await this.memoryGate(g, vectors);
		if (!q.ok) {
			if (g.state === "materialized") await this.store.inTx((c) => idx.reopenGeneration(c, g.generation));
			return;
		}
		if (g.state === "building") {
			await this.store.inTx((c) => idx.markMaterialized(c, g.generation, p.expectedPoints));
			this.log.info("index generation materialized", { generation: g.generation, points: p.expectedPoints });
			return;
		}
		await this.store.inTx(async (c) => {
			const locked = await idx.getGeneration(c, g.generation, true);
			if (locked?.state !== "materialized") return;
			await idx.acceptGeneration(c, g.generation);
			// Revisions indexed while this generation was building become indexed now.
			const r = await c.query<{
				ingest_request_id: string;
				source_id: string;
				source_revision: string;
				tenant_id: string;
			}>(
				`SELECT e.ingest_request_id, e.source_id, e.source_revision::text AS source_revision, s.tenant_id
				 FROM index_entries e JOIN sources s ON s.source_id = e.source_id
				 WHERE e.generation = $1 AND e.state = 'accepted'`,
				[g.generation],
			);
			for (const x of r.rows)
				await this.markIndexed(
					c,
					{ ingestRequestId: x.ingest_request_id, sourceId: x.source_id, sourceRevision: Number(x.source_revision) },
					g.generation,
					x.tenant_id,
					`index-generation-${g.generation}`,
				);
		});
		this.log.info("index generation accepted", { generation: g.generation });
	}

	/**
	 * Readability of a deleted source ended in its deletion transaction; its
	 * points are removed from every generation that still has a collection,
	 * retired ones included, and the source is marked purged only after every
	 * delete was confirmed.
	 */
	private async purge(vectors: VectorIndex, gens: IndexGeneration[]): Promise<void> {
		for (const sourceId of await idx.unpurged(this.store.pool, 20)) {
			try {
				for (const g of gens)
					if (await vectors.exists(g.collectionName))
						await vectors.deleteWhere(g.collectionName, {
							must: [{ key: "source_id", match: { value: sourceId } }],
						});
				await idx.markPurged(this.store.pool, sourceId);
			} catch (err) {
				if (!(err instanceof VectorUnavailable)) throw err;
				this.log.warn("deleted source's points not purged yet", { sourceId, error: err.message });
			}
		}
	}
}
