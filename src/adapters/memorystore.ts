// The PostgresStore projection of MemoryFact (B11, DD-07 §5): the pinned
// @langchain/langgraph-checkpoint-postgres 1.0.5 store as a key/value
// projection in its own vendor schema, with its own pool under the Store
// role. It is never enlisted in a Knowledge transaction and never an
// authority: every value carries the fact revision it projects, a fact that
// is no longer confirmed is projected as a content-free tombstone, and the
// writer (application/memory.ts) checks the authoritative fact before and
// after each write. The runtime never runs DDL (ensureTables: false); the
// vendor schema is prepared by migrateStore under a separate migration
// identity. No index configuration is given, so the Store keeps no hidden
// embeddings: semantic recall uses Qdrant.
import { createHash } from "node:crypto";
import { PostgresStore } from "@langchain/langgraph-checkpoint-postgres/store";
import pg from "pg";

/** What the Store holds for one fact: the confirmed content, or a tombstone without it. */
export interface StoredFact {
	factId: string;
	revision: number;
	state: string;
	tombstone: boolean;
	contentDigest: string;
	content?: string;
	subjectType?: string;
	subjectId?: string;
	scopeId?: string;
	sourceRefs?: string[];
	expiresAt?: string;
}

/** The port the use cases depend on. */
export interface MemoryStorePort {
	get(tenantId: string, factId: string): Promise<StoredFact | undefined>;
	put(tenantId: string, value: StoredFact): Promise<void>;
	/** One page of a tenant's items (the rebuild's reconciliation). */
	list(tenantId: string, offset: number, limit: number): Promise<StoredFact[]>;
	close(): Promise<void>;
}

/** Namespace labels must avoid '.', '%', '_' and ':'; a digest of the tenant does. */
export function namespaceOf(tenantId: string): string[] {
	return ["memory", `t${createHash("sha256").update(tenantId).digest("hex").slice(0, 32)}`];
}

export class StoreUnavailable extends Error {}

function wrap<T>(what: string, p: Promise<T>): Promise<T> {
	return p.catch((err: unknown) => {
		throw new StoreUnavailable(`store ${what}: ${String(err).slice(0, 200)}`);
	});
}

function parse(v: Record<string, unknown> | undefined): StoredFact | undefined {
	if (!v || typeof v.factId !== "string" || typeof v.revision !== "number") return undefined;
	return v as unknown as StoredFact;
}

export class PostgresMemoryStore implements MemoryStorePort {
	private readonly store: PostgresStore;

	constructor(url: string, schema: string, maxConn: number) {
		this.store = new PostgresStore({
			connectionOptions: { connectionString: url, max: maxConn },
			schema,
			ensureTables: false,
		});
	}

	async get(tenantId: string, factId: string): Promise<StoredFact | undefined> {
		const item = await wrap("get", this.store.get(namespaceOf(tenantId), factId));
		return parse(item?.value);
	}

	async put(tenantId: string, value: StoredFact): Promise<void> {
		// index: false — the Store never computes or keeps embeddings.
		await wrap(
			"put",
			this.store.put(namespaceOf(tenantId), value.factId, value as unknown as Record<string, unknown>, false),
		);
	}

	async list(tenantId: string, offset: number, limit: number): Promise<StoredFact[]> {
		const items = await wrap("search", this.store.search(namespaceOf(tenantId), { limit, offset }));
		return items.flatMap((i) => {
			const v = parse(i.value);
			return v ? [v] : [];
		});
	}

	close(): Promise<void> {
		return this.store.stop();
	}
}

/**
 * The vendor migration (Knowledge's store-migrate entry): runs the Store's
 * own migrations under the Store migration identity, which owns the vendor
 * schema, then grants the runtime Store role DML on it and nothing else.
 */
export async function migrateStore(migratorUrl: string, schema: string, runtimeRole: string): Promise<number> {
	if (!/^[a-z][a-z0-9_]{0,62}$/.test(schema) || !/^[a-z][a-z0-9_]{0,62}$/.test(runtimeRole))
		throw new Error("schema and runtime role must be plain lower-case identifiers");
	const vendor = new PostgresStore({ connectionOptions: migratorUrl, schema, ensureTables: true });
	try {
		await vendor.setup();
	} finally {
		await vendor.stop();
	}
	const c = new pg.Client({ connectionString: migratorUrl });
	await c.connect();
	try {
		await c.query(`GRANT USAGE ON SCHEMA "${schema}" TO "${runtimeRole}"`);
		await c.query(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "${schema}" TO "${runtimeRole}"`);
		const r = await c.query<{ v: number }>(`SELECT max(v) AS v FROM "${schema}".store_migrations`);
		return Number(r.rows[0]?.v ?? -1);
	} finally {
		await c.end();
	}
}
