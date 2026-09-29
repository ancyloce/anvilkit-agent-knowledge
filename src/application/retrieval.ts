// RetrievalService.Search (DD-07 §4): hybrid dense/sparse retrieval under
// the caller's current authorization.
//
// Resolve: the snapshot freezes revisions and the generation; the allowed
// set is the snapshot's sources the caller can read now (the Source
// Registry's rule in SQL, never a cache), bounded by the profile: an
// oversized set is refused, never truncated. Embed: the query through the
// reviewed profile of the generation's own vector space; another model's
// space is refused. Search: one Query API call whose dense and sparse
// prefetch branches both carry the allowed-source filter, with the same
// filter outside as additional protection, fused by RRF and ordered with a
// stable tie-break. Every returned point's payload must name an allowed
// revision. Recheck before reading text: candidates whose sources stopped
// being readable are dropped before their text is read or sent to the
// reranker; the text comes from the verified parser result and must prove
// its digest. Rerank, recheck again, then assemble numbered citations
// within the budget. Nothing about withheld candidates reaches the caller:
// no text, score or count. No evidence is an allowed answer.
import { createHash } from "node:crypto";
import * as idx from "../adapters/indexdb.js";
import { InferenceError, InferenceMismatch, type InferencePort } from "../adapters/inference.js";
import type { ObjectStore } from "../adapters/objects.js";
import type * as db from "../adapters/postgres.js";
import { type Filter, type HybridQuery, type VectorIndex, VectorUnavailable } from "../adapters/qdrant.js";
import * as rdb from "../adapters/retrievaldb.js";
import { type SpaceProfile, sameSpace, sourceKeyOf, vectorNames } from "../domain/index.js";
import {
	assembleContext,
	authorizationRevisionOf,
	type Candidate,
	type ContextItem,
	RetrievalError,
	type RetrievalProfile,
	stableOrder,
} from "../domain/retrieval.js";
import { principalsOf, type Scope } from "../domain/source.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import { ChunkTexts, ChunkUnverified } from "./chunktext.js";
import type { Clock } from "./tasks.js";

export interface SearchRequest {
	scope: Scope;
	snapshotId: string;
	query: string;
	maxContextItems: number;
	retrievalProfileId: string;
	deadline: Date;
}

export interface SearchResult {
	items: ContextItem[];
	noAnswer: boolean;
	indexGeneration: number;
	authorizationRevision: string;
	/** Compute calls this search made (the evaluation's cost unit); not part of the public answer. */
	compute: { embeddings: number; rerankCandidates: number };
}

export class Retrieval {
	constructor(
		private readonly store: db.Store,
		private readonly vectors: () => VectorIndex | undefined,
		private readonly inference: () => InferencePort | undefined,
		private readonly objects: () => ObjectStore | undefined,
		private readonly space: () => SpaceProfile | undefined,
		private readonly profile: () => RetrievalProfile | undefined,
		private readonly maxResultBytes: () => number,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	async search(req: SearchRequest): Promise<SearchResult> {
		const started = Date.now();
		try {
			const out = await this.run(req);
			this.metrics.retrievalCompute.inc({ kind: "query_embeddings" }, out.compute.embeddings);
			this.metrics.retrievalCompute.inc({ kind: "rerank_candidates" }, out.compute.rerankCandidates);
			this.metrics.searches.inc({ outcome: out.noAnswer ? "no_answer" : "answered" });
			this.metrics.searchSeconds.observe((Date.now() - started) / 1000);
			return out;
		} catch (err) {
			this.metrics.searches.inc({ outcome: err instanceof RetrievalError ? "refused" : "failed" });
			throw err;
		}
	}

	private readable(scope: Scope, sourceIds: string[]): Promise<Map<string, rdb.Readable>> {
		return rdb.readableNow(this.store.pool, scope.tenantId, scope.projectId, [...principalsOf(scope)], sourceIds);
	}

	private async run(req: SearchRequest): Promise<SearchResult> {
		const profile = this.profile();
		if (!profile || req.retrievalProfileId !== profile.profileId)
			throw new RetrievalError("PROFILE_UNQUALIFIED", `retrieval profile ${req.retrievalProfileId}`);
		const now = this.clock.now().getTime();
		if (req.deadline.getTime() <= now) throw new RetrievalError("DEADLINE_EXCEEDED", "deadline passed");
		// The caller's deadline, never beyond the profile's bound.
		const deadline = Math.min(req.deadline.getTime(), now + profile.maxDeadlineMs);
		const inTime = () => {
			if (this.clock.now().getTime() >= deadline) throw new RetrievalError("DEADLINE_EXCEEDED", "deadline passed");
		};
		const vectors = this.vectors();
		const inference = this.inference();
		const objects = this.objects();
		const space = this.space();
		if (!vectors || !inference || !objects || !space)
			throw new RetrievalError("UNAVAILABLE", "retrieval is not configured");

		// Resolve: the frozen snapshot and the caller's current authorization.
		const snap = await rdb.getSnapshot(this.store.pool, req.snapshotId);
		const scope = req.scope;
		if (!snap || snap.tenantId !== scope.tenantId || (scope.projectId && snap.projectId !== scope.projectId))
			throw new RetrievalError("NOT_FOUND", "snapshot");
		const g = await idx.getGeneration(this.store.pool, snap.generation);
		if (!g) throw new RetrievalError("NOT_FOUND", "snapshot");
		if (!sameSpace(g, space))
			throw new RetrievalError(
				"PROFILE_UNQUALIFIED",
				`generation ${g.generation} is another vector space than the reviewed embedding profile`,
			);
		const readable = await this.readable(
			scope,
			snap.sources.map((s) => s.sourceId),
		);
		const allowed = snap.sources.filter((s) => readable.has(s.sourceId));
		if (allowed.length > profile.maxAllowedSources)
			throw new RetrievalError(
				"SCOPE_TOO_LARGE",
				`${allowed.length} allowed sources exceed the profile's ${profile.maxAllowedSources}`,
			);
		const authorizationOf = (ids: Iterable<string>, from: Map<string, rdb.Readable>) =>
			authorizationRevisionOf([...ids].map((id) => ({ sourceId: id, aclRevision: from.get(id)?.aclRevision ?? 0 })));
		const empty = (auth: string, embeddings = 0): SearchResult => ({
			items: [],
			noAnswer: true,
			indexGeneration: g.generation,
			authorizationRevision: auth,
			compute: { embeddings, rerankCandidates: 0 },
		});
		if (allowed.length === 0) return empty(authorizationOf([], readable));
		const allowedKeys = new Map(allowed.map((s) => [sourceKeyOf(s.sourceId, s.sourceRevision), s]));

		// Embed the query in the generation's space.
		const compute = {
			taskId: `search-${createHash("sha256").update(`${snap.snapshotId}\0${req.query}`).digest("hex").slice(0, 32)}`,
			generation: String(g.generation),
		};
		inTime();
		let q: Awaited<ReturnType<InferencePort["embed"]>>;
		try {
			q = await inference.embed(compute, "query", [req.query]);
		} catch (err) {
			throw this.computeError(err);
		}
		const dense = q.dense[0] as number[];
		const sparse = q.sparse[0] as { indices: number[]; values: number[] };

		// Both prefetch branches and the outer query carry the same allowed-source filter.
		const filter: Filter = {
			must: [
				{ key: "tenant_id", match: { value: scope.tenantId } },
				{ key: "source_key", match: { any: [...allowedKeys.keys()] } },
			],
		};
		if (scope.projectId) filter.must.push({ key: "project_id", match: { value: scope.projectId } });
		const query: HybridQuery = {
			prefetch: [{ query: dense, using: vectorNames.dense, filter, limit: profile.denseLimit }],
			query: { rrf: { k: profile.rrfK } },
			filter,
			limit: profile.fusedLimit,
		};
		// A query without lexical weight has no sparse branch (an empty sparse query matches nothing).
		if (sparse.indices.length > 0)
			query.prefetch.push({ query: sparse, using: vectorNames.sparse, filter, limit: profile.sparseLimit });
		inTime();
		let points: Awaited<ReturnType<VectorIndex["search"]>>;
		try {
			points = await vectors.search(g.collectionName, query);
		} catch (err) {
			if (err instanceof VectorUnavailable) throw new RetrievalError("UNAVAILABLE", "vector index unavailable");
			throw err;
		}
		const fused = stableOrder(
			points.flatMap((p) => {
				const key = String(p.payload.source_key ?? "");
				const chunkId = String(p.payload.chunk_id ?? "");
				// A point outside the allowed set is never a candidate, whatever the index returned.
				if (!allowedKeys.has(key) || p.payload.tenant_id !== scope.tenantId || !chunkId) {
					this.metrics.withheld.inc({ stage: "integrity" });
					return [];
				}
				return [{ chunkId, score: p.score, key }];
			}),
		).slice(0, profile.rerankLimit);
		if (fused.length === 0)
			return empty(
				authorizationOf(
					allowed.map((s) => s.sourceId),
					readable,
				),
				1,
			);

		// Recheck before any text is read or sent to the reranker.
		inTime();
		const beforeRead = await this.readable(scope, [
			...new Set(fused.map((f) => (allowedKeys.get(f.key) as { sourceId: string }).sourceId)),
		]);
		const rows = await idx.chunksById(
			this.store.pool,
			fused.map((f) => f.chunkId),
		);
		const texts = new ChunkTexts(objects, this.maxResultBytes());
		const candidates: Candidate[] = [];
		for (const f of fused) {
			const frozen = allowedKeys.get(f.key) as { sourceId: string; sourceRevision: number };
			if (!beforeRead.has(frozen.sourceId)) {
				this.metrics.withheld.inc({ stage: "before_read" });
				continue;
			}
			const row = rows.get(f.chunkId);
			if (!row || row.sourceId !== frozen.sourceId || row.sourceRevision !== frozen.sourceRevision) {
				this.metrics.withheld.inc({ stage: "integrity" });
				continue;
			}
			try {
				candidates.push({
					chunkId: row.chunkId,
					sourceId: row.sourceId,
					sourceRevision: row.sourceRevision,
					locator: row.locator,
					contentDigest: row.contentDigest,
					text: await texts.text(row),
					score: 0,
				});
			} catch (err) {
				if (!(err instanceof ChunkUnverified)) throw err;
				this.metrics.withheld.inc({ stage: "integrity" });
				this.log.warn("retrieval candidate unverified", { chunkId: row.chunkId });
			}
		}
		if (candidates.length === 0) return empty(authorizationOf(beforeRead.keys(), beforeRead), 1);

		// Rerank with the fixed reranker.
		inTime();
		let scores: Map<string, number>;
		try {
			scores = await inference.rerank(
				compute,
				req.query,
				candidates.map((c) => ({ candidateId: c.chunkId, text: c.text })),
			);
		} catch (err) {
			throw this.computeError(err);
		}
		for (const c of candidates) c.score = scores.get(c.chunkId) ?? Number.NEGATIVE_INFINITY;

		// Recheck immediately before disclosure.
		inTime();
		const beforeReturn = await this.readable(scope, [...new Set(candidates.map((c) => c.sourceId))]);
		const disclosed = candidates.filter((c) => {
			if (beforeReturn.has(c.sourceId)) return true;
			this.metrics.withheld.inc({ stage: "before_return" });
			return false;
		});
		const items = assembleContext(disclosed, req.maxContextItems, profile.maxContextChars, profile.minRerankScore);
		return {
			items,
			noAnswer: items.length === 0,
			indexGeneration: g.generation,
			authorizationRevision: authorizationOf(beforeReturn.keys(), beforeReturn),
			compute: { embeddings: 1, rerankCandidates: candidates.length },
		};
	}

	private computeError(err: unknown): Error {
		if (err instanceof InferenceMismatch)
			return new RetrievalError("PROFILE_UNQUALIFIED", "inference answer does not bind");
		if (err instanceof InferenceError)
			return err.retryable
				? new RetrievalError("UNAVAILABLE", "inference unavailable")
				: new RetrievalError("INVALID_ARGUMENT", `inference refused the query (${err.code})`);
		return err instanceof Error ? err : new Error(String(err));
	}
}
