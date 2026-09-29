// Knowledge's native Qdrant adapter (B06/B07, DD-07 §3): named dense and
// sparse vectors per point, explicit keyword payload indexes, writes that
// wait for processing under the configured ordering, reads under the
// configured consistency and one atomic alias switch. The generic LangChain
// VectorStore is not used: it does not express sparse named vectors or the
// per-branch filters retrieval needs. The client does no transparent
// retries; a failed or unanswered write is repeated by the caller under the
// same deterministic point ids. Knowledge is the only client and Qdrant is a
// projection: nothing read here is authorization.
import { QdrantClient } from "@qdrant/js-client-rest";
import {
	alias,
	type IndexGeneration,
	type PointPayload,
	payloadIndexes,
	vectorNames,
} from "../domain/index.js";

export interface QdrantSettings {
	url: string;
	apiKey: string;
	timeoutMs: number;
	replicationFactor: number;
	writeConsistencyFactor: number;
	writeOrdering: "weak" | "medium" | "strong";
	readConsistency: "all" | "majority" | "quorum";
}

export interface Point {
	id: string;
	dense: number[];
	sparse: { indices: number[]; values: number[] };
	payload: PointPayload;
}

export interface StoredPoint {
	id: string;
	payload: Record<string, unknown> | null;
	hasDense: boolean;
	hasSparse: boolean;
}

/** A keyword condition list; every filter Knowledge sends is built from these. */
export interface Filter {
	must: ({ key: string; match: { value: string | number } | { any: string[] } } | { has_id: string[] })[];
}

export interface ScoredPoint {
	id: string;
	score: number;
	payload: Record<string, unknown>;
}

/** The hybrid query body: two filtered prefetch branches fused by RRF, with the outer filter. */
export interface HybridQuery {
	prefetch: {
		query: number[] | { indices: number[]; values: number[] };
		using: string;
		filter: Filter;
		limit: number;
	}[];
	query: { rrf: { k: number } };
	filter: Filter;
	limit: number;
}

/** A refused or failed Qdrant call: the caller decides whether the same write is repeated. */
export class VectorUnavailable extends Error {}

/** The port the use cases depend on. */
export interface VectorIndex {
	ensureCollection(g: IndexGeneration): Promise<void>;
	upsert(collection: string, points: Point[]): Promise<void>;
	retrieve(collection: string, ids: string[]): Promise<Map<string, StoredPoint>>;
	count(collection: string, filter: Filter): Promise<number>;
	deleteIds(collection: string, ids: string[]): Promise<void>;
	deleteWhere(collection: string, filter: Filter): Promise<void>;
	search(collection: string, q: HybridQuery): Promise<ScoredPoint[]>;
	aliasTarget(): Promise<string | undefined>;
	switchAlias(collection: string): Promise<void>;
	exists(collection: string): Promise<boolean>;
	close(): void;
}

function wrap<T>(what: string, p: Promise<T>): Promise<T> {
	return p.catch((err: unknown) => {
		const status = (err as { status?: number })?.status;
		throw new VectorUnavailable(`qdrant ${what}${status ? ` ${status}` : ""}: ${String(err).slice(0, 200)}`);
	});
}

export class QdrantIndex implements VectorIndex {
	private readonly client: QdrantClient;

	constructor(private readonly s: QdrantSettings) {
		// checkCompatibility off: the version is pinned in the foundation and
		// its check is an extra request at construction, not a guarantee.
		this.client = new QdrantClient({
			url: s.url,
			apiKey: s.apiKey,
			timeout: s.timeoutMs,
			checkCompatibility: false,
		});
	}

	async exists(collection: string): Promise<boolean> {
		return (await wrap("exists", this.client.collectionExists(collection))).exists;
	}

	/**
	 * Creates the generation's collection once (dense cosine vectors of the
	 * profile's dimensions, one sparse vector, the configured replication
	 * and write consistency) and its keyword payload indexes; an existing
	 * collection must carry exactly that vector space.
	 */
	async ensureCollection(g: IndexGeneration): Promise<void> {
		if (await this.exists(g.collectionName)) {
			const info = await wrap("get collection", this.client.getCollection(g.collectionName));
			const vectors = info.config.params.vectors as Record<string, { size?: number; distance?: string }> | undefined;
			const dense = vectors?.[vectorNames.dense];
			const sparse = (info.config.params.sparse_vectors ?? {}) as Record<string, unknown>;
			if (dense?.size !== g.dimensions || dense.distance !== "Cosine" || !(vectorNames.sparse in sparse))
				throw new VectorUnavailable(`collection ${g.collectionName} does not carry generation ${g.generation}'s space`);
		} else {
			await wrap(
				"create collection",
				this.client.createCollection(g.collectionName, {
					vectors: { [vectorNames.dense]: { size: g.dimensions, distance: "Cosine" } },
					sparse_vectors: { [vectorNames.sparse]: {} },
					replication_factor: this.s.replicationFactor,
					write_consistency_factor: this.s.writeConsistencyFactor,
				}),
			).catch(async (err) => {
				// A concurrent creator won: the check above runs again.
				if (!(await this.exists(g.collectionName))) throw err;
			});
		}
		for (const [field, schema] of Object.entries(payloadIndexes))
			await wrap(
				"payload index",
				this.client.createPayloadIndex(g.collectionName, {
					field_name: field,
					field_schema: schema,
					wait: true,
					ordering: this.s.writeOrdering,
				}),
			);
	}

	async upsert(collection: string, points: Point[]): Promise<void> {
		if (points.length === 0) return;
		const r = await wrap(
			"upsert",
			this.client.upsert(collection, {
				wait: true,
				ordering: this.s.writeOrdering,
				points: points.map((p) => ({
					id: p.id,
					vector: { [vectorNames.dense]: p.dense, [vectorNames.sparse]: p.sparse },
					payload: p.payload as unknown as Record<string, unknown>,
				})),
			}),
		);
		// wait confirms processing of this write, not a read guarantee: the
		// caller still reads the points back before it trusts them.
		if (r.status !== "completed") throw new VectorUnavailable(`upsert ${r.status}`);
	}

	async retrieve(collection: string, ids: string[]): Promise<Map<string, StoredPoint>> {
		const out = new Map<string, StoredPoint>();
		if (ids.length === 0) return out;
		const rows = await wrap(
			"retrieve",
			this.client.retrieve(collection, {
				ids,
				with_payload: true,
				with_vector: [vectorNames.dense, vectorNames.sparse],
				consistency: this.s.readConsistency,
			}),
		);
		for (const r of rows) {
			const v = (r.vector ?? {}) as Record<string, unknown>;
			const dense = v[vectorNames.dense];
			const sparse = v[vectorNames.sparse] as { indices?: unknown[] } | undefined;
			out.set(String(r.id), {
				id: String(r.id),
				payload: (r.payload ?? null) as Record<string, unknown> | null,
				hasDense: Array.isArray(dense) && dense.length > 0,
				// An empty sparse vector (a chunk without lexical weight) is still present.
				hasSparse: sparse !== undefined && Array.isArray(sparse.indices),
			});
		}
		return out;
	}

	async count(collection: string, filter: Filter): Promise<number> {
		return (await wrap("count", this.client.count(collection, { filter, exact: true }))).count;
	}

	async deleteIds(collection: string, ids: string[]): Promise<void> {
		if (ids.length === 0) return;
		await wrap("delete", this.client.delete(collection, { wait: true, ordering: this.s.writeOrdering, points: ids }));
	}

	async deleteWhere(collection: string, filter: Filter): Promise<void> {
		await wrap("delete", this.client.delete(collection, { wait: true, ordering: this.s.writeOrdering, filter }));
	}

	async search(collection: string, q: HybridQuery): Promise<ScoredPoint[]> {
		const r = await wrap(
			"query",
			this.client.query(collection, {
				prefetch: q.prefetch,
				query: q.query,
				filter: q.filter,
				limit: q.limit,
				with_payload: true,
				with_vector: false,
				consistency: this.s.readConsistency,
			}),
		);
		return r.points.map((p) => ({
			id: String(p.id),
			score: p.score,
			payload: (p.payload ?? {}) as Record<string, unknown>,
		}));
	}

	async aliasTarget(): Promise<string | undefined> {
		const r = await wrap("aliases", this.client.getAliases());
		return r.aliases.find((a) => a.alias_name === alias)?.collection_name;
	}

	/** Moves the stable alias in one request: delete and create are applied together. */
	async switchAlias(collection: string): Promise<void> {
		const current = await this.aliasTarget();
		if (current === collection) return;
		const actions: Parameters<QdrantClient["updateCollectionAliases"]>[0]["actions"] = [];
		if (current) actions.push({ delete_alias: { alias_name: alias } });
		actions.push({ create_alias: { collection_name: collection, alias_name: alias } });
		await wrap("alias", this.client.updateCollectionAliases({ actions }));
	}

	close(): void {}
}
