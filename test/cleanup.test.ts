// P2.7 (AC1, AC9): the erasure of a deleted source and the retention of
// delivery rows under the cleanup identity, and the one-replica reconciler
// lock, on real PostgreSQL 17.
import pg from "pg";
import { Registry } from "prom-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import { newPool, Store } from "../src/adapters/postgres.js";
import { Cleanup, withLock } from "../src/application/cleanup.js";
import { silentLogger } from "../src/log.js";
import { Metrics } from "../src/metrics.js";
import { FakeClock, type Instance, startInstance } from "./harness.js";
import { seedParsed } from "./rag.js";

const clock = new FakeClock(new Date("2026-10-13T12:00:00Z"));
const objects = new MemoryObjects();
let inst: Instance;
let store: Store;
let cleanupPool: pg.Pool;
let cleanup: Cleanup;
const D = `sha256:${"a".repeat(64)}`;

beforeAll(async () => {
	inst = await startInstance();
	store = new Store(newPool(inst.appUrl, 4));
	cleanupPool = newPool(inst.cleanupUrl, 2);
	cleanup = new Cleanup(
		store,
		() => cleanupPool,
		() => objects,
		() => ({
			batch: 100,
			outboxRetentionMs: 3_600_000,
			inboxRetentionMs: 3_600_000,
			attemptsRetentionMs: 3_600_000,
			launchesRetentionMs: 3_600_000,
			consumerGroup: "anvilkit-agent-knowledge-forwarder",
		}),
		clock,
		silentLogger,
		new Metrics(new Registry()),
	);
}, 180_000);

afterAll(async () => {
	await cleanupPool?.end();
	await store?.pool.end();
	await inst?.stop();
});

async function request(taskId: string, state: string, at: string): Promise<void> {
	await inst.admin(
		`INSERT INTO background_requests (task_id, generation, tenant_id, task_kind, input_digest, input, state, result_profile, authorization_ref, correlation_id, updated_at)
		 VALUES ($1, 1, 'tenant_a', 'knowledge-ingest', $2, '{}', $3, 'knowledge-ingest-v1', 'source:x', 'c', $4)`,
		[taskId, D, state, at],
	);
}

describe("erasure of a deleted source (P2.7 AC1)", () => {
	it("removes the upload, the unshared copy, the parse results and the chunk rows, keeps a shared copy, then marks it purged", async () => {
		const gone = await seedParsed(inst, objects, { sourceId: "src-gone", tenantId: "tenant_a", texts: ["one", "two"] });
		await seedParsed(inst, objects, { sourceId: "src-kept", tenantId: "tenant_a", texts: ["three"] });
		// The deleted source's own copy, and one content-addressed copy both sources share.
		await inst.admin("UPDATE source_revisions SET object_key = 'sources/tenant_a/own' WHERE source_id = 'src-gone'");
		await inst.admin(
			"INSERT INTO source_revisions (source_id, revision, content_digest, media_type, size_bytes, object_key) VALUES ('src-gone', 2, $1, 'text/markdown', 1, 'sources/tenant_a/shared')",
			[D],
		);
		await inst.admin("UPDATE source_revisions SET object_key = 'sources/tenant_a/shared' WHERE source_id = 'src-kept'");
		for (const k of ["uploads/tenant_a/x.md", "sources/tenant_a/own", "sources/tenant_a/shared"])
			objects.objects.set(k, Buffer.from(k));
		await request("ingest-src-gone-r1", "accepted", "2026-10-13T11:00:00Z");
		await inst.admin(
			`INSERT INTO parse_launches (task_id, generation, attempt, launch_id, launch_key, worker_id, profile_id, profile_revision, input_digest, deadline, state, result_key, result_digest, verdict)
			 VALUES ('ingest-src-gone-r1', 1, 1, 'launch-gone', 'parse-gone', 'w1', 'parser-docling-dev-v1', 1, $1, now(), 'completed', 'parse/parse-gone/result.json', $1, 'parsed')`,
			[D],
		);
		objects.objects.set("parse/parse-gone/result.json", Buffer.from("{}"));
		expect(await cleanup.purgeDeleted()).toBe(0);
		expect((await cleanup.status("src-gone"))?.status).toBe("live");
		// Deleted, and the indexer has removed its points.
		await inst.admin("UPDATE sources SET deleted = true, deleted_at = now() WHERE source_id = 'src-gone'");
		expect(await cleanup.purgeDeleted()).toBe(0);
		expect((await cleanup.status("src-gone"))?.status).toBe("pending");
		await inst.admin("UPDATE sources SET points_purged_at = now() WHERE source_id = 'src-gone'");
		expect(await cleanup.purgeDeleted()).toBe(1);
		expect(objects.objects.has("uploads/tenant_a/x.md")).toBe(false);
		expect(objects.objects.has("sources/tenant_a/own")).toBe(false);
		expect(objects.objects.has("parse/parse-gone/result.json")).toBe(false);
		expect(objects.objects.has("sources/tenant_a/shared")).toBe(true);
		const chunks = await inst.admin("SELECT count(*)::int AS n FROM chunks WHERE source_id = $1", [gone.sourceId]);
		expect(chunks.rows[0].n).toBe(0);
		const kept = await inst.admin("SELECT count(*)::int AS n FROM chunks WHERE source_id = 'src-kept'");
		expect(kept.rows[0].n).toBe(1);
		const status = await cleanup.status("src-gone");
		expect(status).toMatchObject({ status: "purged" });
		expect(status?.purgedAt).toBeInstanceOf(Date);
		expect(await cleanup.purgeDeleted()).toBe(0);
	});
});

describe("delivery retention (P2.7 AC9)", () => {
	it("prunes acknowledged outbox rows, old inbox entries, and the attempts and launches of old terminal requests; keeps the rest", async () => {
		const old = "2026-10-13T10:00:00Z";
		const recent = "2026-10-13T11:59:00Z";
		for (const [i, at] of [
			[1, old],
			[2, old],
			[3, recent],
		] as const)
			await inst.admin(
				`INSERT INTO outbox (uuid, payload, metadata, transaction_id, created_at) VALUES ($1, '{}', '{}', pg_current_xact_id(), $2)`,
				[`0192a1b2-3c4d-7e5f-8a9b-0c1d2e3f4a${String(i).padStart(2, "0")}`, at],
			);
		// The forwarder acknowledged everything so far.
		await inst.admin(
			`INSERT INTO outbox_offsets (consumer_group, offset_acked, last_processed_transaction_id)
			 SELECT 'anvilkit-agent-knowledge-forwarder', max("offset"), max(transaction_id) FROM outbox`,
		);
		await inst.admin(
			`INSERT INTO outbox (uuid, payload, metadata, transaction_id, created_at) VALUES ('0192a1b2-3c4d-7e5f-8a9b-0c1d2e3f4a99', '{}', '{}', pg_current_xact_id(), $1)`,
			[old],
		);
		await inst.admin(
			"INSERT INTO inbox (event_id, consumer, accepted_at) VALUES (gen_random_uuid(), 'c', $1), (gen_random_uuid(), 'c', $2)",
			[old, recent],
		);
		await request("t-done", "accepted", old);
		await request("t-open", "leased", old);
		for (const t of ["t-done", "t-open"])
			await inst.admin(
				"INSERT INTO task_attempts (task_id, generation, ordinal, worker_id, lease_until) VALUES ($1, 1, 1, 'w', now())",
				[t],
			);
		await inst.admin("UPDATE parse_launches SET updated_at = $1 WHERE launch_key = 'parse-gone'", [old]);
		const out = await cleanup.retain();
		expect(out).toMatchObject({ outbox: 2, inbox: 1, attempts: 1 });
		const left = await inst.admin("SELECT uuid FROM outbox ORDER BY uuid");
		expect(left.rows.map((r) => r.uuid)).toEqual([
			"0192a1b2-3c4d-7e5f-8a9b-0c1d2e3f4a03",
			"0192a1b2-3c4d-7e5f-8a9b-0c1d2e3f4a99",
		]);
		const attempts = await inst.admin("SELECT task_id FROM task_attempts WHERE task_id LIKE 't-%'");
		expect(attempts.rows.map((r) => r.task_id)).toEqual(["t-open"]);
		// The launch of the purged source's terminal parse (accepted at 11:00) is past retention too.
		expect(out.launches).toBe(1);
	});
});

describe("one reconciler (P2.7 AC9)", () => {
	it("runs the shared passes on exactly one holder of the advisory lock", async () => {
		const a = new pg.Pool({ connectionString: inst.appUrl, max: 2 });
		const b = new pg.Pool({ connectionString: inst.appUrl, max: 2 });
		try {
			let release!: () => void;
			const held = new Promise<void>((r) => {
				release = r;
			});
			let ran = 0;
			const first = withLock(a, 42n, async () => {
				ran++;
				await held;
			});
			await new Promise((r) => setTimeout(r, 200));
			expect(await withLock(b, 42n, async () => void ran++)).toBe(false);
			release();
			expect(await first).toBe(true);
			expect(await withLock(b, 42n, async () => void ran++)).toBe(true);
			expect(ran).toBe(2);
		} finally {
			await a.end();
			await b.end();
		}
	});
});
