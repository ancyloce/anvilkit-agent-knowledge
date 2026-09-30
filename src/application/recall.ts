// MemoryService.RecallFacts (DD-07 §5, §4): semantic recall of confirmed
// facts through the qualified retrieval path of P16, under the caller's
// current authority.
//
// Resolve: the facts the caller may recall now come from the authoritative
// table (visible to the caller, confirmed, not deleted, not expired) and
// their provenance must still be readable by the caller at its current
// revision in the Source Registry. The allowed set is bounded; a larger
// set is refused, never truncated. Search: the query is embedded in the
// accepted generation's space, and one Query API call's dense and sparse
// prefetch branches and outer query all carry the allowed fact@revision
// keys; a point outside that set is never a candidate, whatever Qdrant
// returned. Recheck: before any content is read (and sent to the reranker)
// and again immediately before return; content always comes from the
// authoritative record at the exact revision, never from Qdrant or the
// Store. Withheld facts leave no trace in the answer. No fact above the
// profile's floor is an allowed answer.
import { createHash } from "node:crypto";
import * as idx from "../adapters/indexdb.js";
import { InferenceError, InferenceMismatch, type InferencePort } from "../adapters/inference.js";
import * as mdb from "../adapters/memorydb.js";
import type * as db from "../adapters/postgres.js";
import { type Filter, type HybridQuery, type VectorIndex, VectorUnavailable } from "../adapters/qdrant.js";
import * as rdb from "../adapters/retrievaldb.js";
import { type SpaceProfile, sameSpace, vectorNames } from "../domain/index.js";
import { type Fact, MemoryError, memoryKeyOf, parseSourceRef } from "../domain/memory.js";
import { type RetrievalProfile, stableOrder } from "../domain/retrieval.js";
import { principalsOf, type Scope } from "../domain/source.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import type { MemoryBounds } from "./memory.js";
import type { Clock } from "./tasks.js";

export interface RecallRequest {
	scope: Scope;
	query: string;
	maxFacts: number;
	retrievalProfileId: string;
	deadline: Date;
}

export interface RecallResult {
	facts: Fact[];
	noAnswer: boolean;
	indexGeneration: number;
	/** Compute this recall spent (the evaluation's cost unit); not part of the public answer. */
	compute: { embeddings: number; rerankCandidates: number };
}

export class Recall {
	constructor(
		private readonly store: db.Store,
		private readonly vectors: () => VectorIndex | undefined,
		private readonly inference: () => InferencePort | undefined,
		private readonly space: () => SpaceProfile | undefined,
		private readonly profile: () => RetrievalProfile | undefined,
		private readonly bounds: () => MemoryBounds,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	async recall(req: RecallRequest): Promise<RecallResult> {
		try {
			const out = await this.run(req);
			this.metrics.retrievalCompute.inc({ kind: "query_embeddings" }, out.compute.embeddings);
			this.metrics.retrievalCompute.inc({ kind: "rerank_candidates" }, out.compute.rerankCandidates);
			this.metrics.recalls.inc({ outcome: out.noAnswer ? "no_answer" : "answered" });
			return out;
		} catch (err) {
			this.metrics.recalls.inc({ outcome: err instanceof MemoryError ? "refused" : "failed" });
			if (!(err instanceof MemoryError)) this.log.warn("memory recall failed", { error: String(err).slice(0, 200) });
			throw err;
		}
	}

	/**
	 * The facts the scope may recall now, by id, at their current revision:
	 * the table's rule plus readable, current provenance. `limit` bounds the
	 * read; reaching it refuses the recall instead of truncating the set.
	 */
	private async allowed(scope: Scope, factIds?: string[]): Promise<Map<string, mdb.AllowedFact>> {
		const bound = this.bounds().maxAllowedFacts;
		const rows = await mdb.recallableFor(this.store.pool, scope, this.clock.now(), bound + 1, factIds);
		if (rows.length > bound)
			throw new MemoryError("SCOPE_TOO_LARGE", `more than ${bound} recallable facts; the set is never truncated`);
		const refs = rows.flatMap((r) => r.sourceRefs.map(parseSourceRef));
		const readable = await rdb.readableNow(
			this.store.pool,
			scope.tenantId,
			scope.projectId,
			[...principalsOf(scope)],
			[...new Set(refs.map((r) => r.sourceId))],
		);
		const out = new Map<string, mdb.AllowedFact>();
		for (const r of rows)
			if (r.sourceRefs.map(parseSourceRef).every((x) => readable.get(x.sourceId)?.currentRevision === x.revision))
				out.set(r.factId, r);
		return out;
	}

	private async run(req: RecallRequest): Promise<RecallResult> {
		const profile = this.profile();
		if (!profile || req.retrievalProfileId !== profile.profileId)
			throw new MemoryError("PROFILE_UNQUALIFIED", `retrieval profile ${req.retrievalProfileId}`);
		const now = this.clock.now().getTime();
		if (req.deadline.getTime() <= now) throw new MemoryError("DEADLINE_EXCEEDED", "deadline passed");
		const deadline = Math.min(req.deadline.getTime(), now + profile.maxDeadlineMs);
		const inTime = () => {
			if (this.clock.now().getTime() >= deadline) throw new MemoryError("DEADLINE_EXCEEDED", "deadline passed");
		};
		const vectors = this.vectors();
		const inference = this.inference();
		const space = this.space();
		if (!vectors || !inference || !space) throw new MemoryError("UNAVAILABLE", "recall is not configured");
		const g = (await idx.listGenerations(this.store.pool)).find((x) => x.state === "accepted");
		if (!g) throw new MemoryError("UNAVAILABLE", "no accepted index generation");
		if (!sameSpace(g, space))
			throw new MemoryError("PROFILE_UNQUALIFIED", `generation ${g.generation} is another vector space`);
		const empty = (embeddings = 0): RecallResult => ({
			facts: [],
			noAnswer: true,
			indexGeneration: g.generation,
			compute: { embeddings, rerankCandidates: 0 },
		});

		// Resolve the allowed set from the authority.
		const allowed = await this.allowed(req.scope);
		if (allowed.size === 0) return empty();
		const keys = new Map([...allowed.values()].map((a) => [memoryKeyOf(a.factId, a.revision), a]));

		// Embed the query in the generation's space.
		inTime();
		let q: Awaited<ReturnType<InferencePort["embed"]>>;
		try {
			q = await inference.embed(
				{
					taskId: `recall-${createHash("sha256").update(`${req.scope.tenantId}\0${req.scope.actorId}\0${req.query}`).digest("hex").slice(0, 32)}`,
					generation: String(g.generation),
				},
				"query",
				[req.query],
			);
		} catch (err) {
			throw computeError(err);
		}
		const dense = q.dense[0] as number[];
		const sparse = q.sparse[0] as { indices: number[]; values: number[] };
		const filter: Filter = {
			must: [
				{ key: "tenant_id", match: { value: req.scope.tenantId } },
				{ key: "kind", match: { value: "memory" } },
				{ key: "memory_key", match: { any: [...keys.keys()] } },
			],
		};
		const query: HybridQuery = {
			prefetch: [{ query: dense, using: vectorNames.dense, filter, limit: profile.denseLimit }],
			query: { rrf: { k: profile.rrfK } },
			filter,
			limit: profile.fusedLimit,
		};
		if (sparse.indices.length > 0)
			query.prefetch.push({ query: sparse, using: vectorNames.sparse, filter, limit: profile.sparseLimit });
		inTime();
		let points: Awaited<ReturnType<VectorIndex["search"]>>;
		try {
			points = await vectors.search(g.collectionName, query);
		} catch (err) {
			if (err instanceof VectorUnavailable) throw new MemoryError("UNAVAILABLE", "vector index unavailable");
			throw err;
		}
		const fused = stableOrder(
			points.flatMap((p) => {
				const key = String(p.payload.memory_key ?? "");
				const a = keys.get(key);
				if (!a || p.payload.tenant_id !== req.scope.tenantId || p.payload.kind !== "memory") {
					this.metrics.withheld.inc({ stage: "integrity" });
					return [];
				}
				return [{ chunkId: a.factId, score: p.score, key }];
			}),
		).slice(0, profile.rerankLimit);
		if (fused.length === 0) return empty(1);

		// Recheck before any content is read or sent to the reranker.
		inTime();
		const beforeRead = await this.allowed(
			req.scope,
			fused.map((f) => f.chunkId),
		);
		const facts = await mdb.getFacts(
			this.store.pool,
			fused.filter((f) => beforeRead.has(f.chunkId)).map((f) => f.chunkId),
		);
		const candidates: { fact: Fact; score: number; chunkId: string }[] = [];
		for (const f of fused) {
			const fact = facts.get(f.chunkId);
			const now = beforeRead.get(f.chunkId);
			// The point's exact revision must still be the recallable one.
			if (!fact || !now || memoryKeyOf(fact.factId, fact.revision) !== f.key || now.revision !== fact.revision) {
				this.metrics.withheld.inc({ stage: "before_read" });
				continue;
			}
			candidates.push({ fact, score: 0, chunkId: fact.factId });
		}
		if (candidates.length === 0) return empty(1);

		inTime();
		let scores: Map<string, number>;
		try {
			scores = await inference.rerank(
				{ taskId: `recall-${g.generation}`, generation: String(g.generation) },
				req.query,
				candidates.map((c) => ({ candidateId: c.fact.factId, text: c.fact.content })),
			);
		} catch (err) {
			throw computeError(err);
		}
		for (const c of candidates) c.score = scores.get(c.fact.factId) ?? Number.NEGATIVE_INFINITY;

		// Recheck immediately before disclosure.
		inTime();
		const beforeReturn = await this.allowed(
			req.scope,
			candidates.map((c) => c.fact.factId),
		);
		const disclosed = stableOrder(
			candidates.filter((c) => {
				if (beforeReturn.get(c.fact.factId)?.revision === c.fact.revision) return true;
				this.metrics.withheld.inc({ stage: "before_return" });
				return false;
			}),
		)
			.filter((c) => c.score >= profile.minRerankScore)
			.slice(0, req.maxFacts)
			.map((c) => c.fact);
		return {
			facts: disclosed,
			noAnswer: disclosed.length === 0,
			indexGeneration: g.generation,
			compute: { embeddings: 1, rerankCandidates: candidates.length },
		};
	}
}

function computeError(err: unknown): Error {
	if (err instanceof InferenceMismatch) return new MemoryError("PROFILE_UNQUALIFIED", "inference answer does not bind");
	if (err instanceof InferenceError)
		return err.retryable
			? new MemoryError("UNAVAILABLE", "inference unavailable")
			: new MemoryError("INVALID_ARGUMENT", `inference refused the query (${err.code})`);
	return err instanceof Error ? err : new Error(String(err));
}
