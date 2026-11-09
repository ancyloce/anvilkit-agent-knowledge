// RB-A25: index freshness (platform.md SLO row 5) on real PostgreSQL 17.
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as idx from "../src/adapters/indexdb.js";
import { MemoryObjects } from "../src/adapters/objects.js";
import { collectionOf } from "../src/domain/index.js";
import { type Instance, startInstance } from "./harness.js";
import { seedParsed, space } from "./rag.js";

const digest = `sha256:${"a".repeat(64)}`;
let inst: Instance;
let pool: pg.Pool;

beforeAll(async () => {
	inst = await startInstance();
	pool = new pg.Pool({ connectionString: inst.appUrl });
}, 180_000);

afterAll(async () => {
	await pool?.end();
	await inst?.stop();
});

describe("index freshness", () => {
	it("counts the eligible revisions the accepted generation has not indexed, and the oldest one's age", async () => {
		const objects = new MemoryObjects();
		await seedParsed(inst, objects, { sourceId: "src_fresh_a", tenantId: "tenant_a", texts: ["alpha"] });
		await seedParsed(inst, objects, { sourceId: "src_fresh_b", tenantId: "tenant_a", texts: ["beta", "gamma"] });
		const c = await pool.connect();
		let generation: number;
		try {
			generation = await idx.insertGeneration(c, space, collectionOf);
			await idx.markMaterialized(c, generation, 1);
			await idx.acceptGeneration(c, generation);
		} finally {
			c.release();
		}
		const accepted = (await idx.listGenerations(pool)).find((g) => g.state === "accepted");
		expect(accepted?.generation).toBe(generation);
		if (!accepted) return;

		let lag = await idx.indexLag(pool, accepted);
		expect(lag.revisions).toBe(2);
		expect(lag.oldestSeconds).toBeGreaterThanOrEqual(0);

		await inst.admin(
			`INSERT INTO index_entries (generation, source_id, source_revision, ingest_request_id, task_id, state, chunk_count, point_count, manifest_digest)
			 VALUES ($1, 'src_fresh_a', 1, 'ing-src_fresh_a-r1', 'index-fresh-a', 'accepted', 1, 1, $2)`,
			[generation, digest],
		);
		lag = await idx.indexLag(pool, accepted);
		expect(lag.revisions).toBe(1);

		await inst.admin(
			`INSERT INTO index_entries (generation, source_id, source_revision, ingest_request_id, task_id, state, chunk_count, failure_code)
			 VALUES ($1, 'src_fresh_b', 1, 'ing-src_fresh_b-r1', 'index-fresh-b', 'quarantined', 2, 'EMBEDDING_FAILED')`,
			[generation],
		);
		lag = await idx.indexLag(pool, accepted);
		expect(lag).toEqual({ revisions: 0, oldestSeconds: 0 });
	});
});
