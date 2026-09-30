// P17-02: the PostgresStore vendor schema is prepared by the Store's own
// migrations under the separate Store migration identity; the runtime Store
// (ensureTables: false) has DML there and nothing else, and never creates
// tables itself.
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrateStore, namespaceOf, PostgresMemoryStore } from "../src/adapters/memorystore.js";
import { type Instance, startInstance } from "./harness.js";

let inst: Instance;

beforeAll(async () => {
	inst = await startInstance();
});
afterAll(async () => {
	await inst?.stop();
});

async function as(url: string, sql: string): Promise<void> {
	const c = new pg.Client({ connectionString: url });
	await c.connect();
	try {
		await c.query(sql);
	} finally {
		await c.end();
	}
}

describe("the memory Store's vendor migration", () => {
	it("is not created by the runtime Store before the vendor migration ran", async () => {
		const store = new PostgresMemoryStore(inst.storeUrl, "memory_store", 2);
		await expect(store.get("tenant_a", "mem-1")).rejects.toThrow();
		await store.close();
		const r = await inst.admin(
			"SELECT count(*)::int AS n FROM information_schema.schemata WHERE schema_name = 'memory_store'",
		);
		expect(r.rows[0].n).toBe(0);
	});

	it("runs the Store's own migrations under its identity and grants the runtime role DML only", async () => {
		const version = await migrateStore(inst.storeMigratorUrl, "memory_store", "anvilkit_knowledge_store");
		expect(version).toBe(3); // migrations 0-3 of the pinned Store without an index configuration
		expect(await migrateStore(inst.storeMigratorUrl, "memory_store", "anvilkit_knowledge_store")).toBe(3);
		const store = new PostgresMemoryStore(inst.storeUrl, "memory_store", 2);
		await store.put("tenant:a.b", {
			factId: "mem-1",
			revision: 2,
			state: "confirmed",
			tombstone: false,
			contentDigest: `sha256:${"a".repeat(64)}`,
			content: "prefers dark previews",
		});
		expect((await store.get("tenant:a.b", "mem-1"))?.revision).toBe(2);
		expect(await store.get("other", "mem-1")).toBeUndefined();
		expect((await store.list("tenant:a.b", 0, 10)).map((v) => v.factId)).toEqual(["mem-1"]);
		expect(namespaceOf("tenant:a.b")[1]).toMatch(/^t[0-9a-f]{32}$/);
		await store.close();
		await expect(as(inst.storeUrl, 'CREATE TABLE "memory_store".forbidden (x int)')).rejects.toThrow();
		await expect(as(inst.storeUrl, "SELECT count(*) FROM memory_facts")).rejects.toThrow();
		await expect(as(inst.appUrl, 'SELECT count(*) FROM "memory_store".store')).rejects.toThrow();
		// No embeddings table: the Store keeps no hidden vectors.
		const t = await inst.admin(
			"SELECT tablename AS table_name FROM pg_tables WHERE schemaname = 'memory_store' ORDER BY tablename",
		);
		expect(t.rows.map((x) => x.table_name)).toEqual(["store", "store_migrations"]);
	});
});
