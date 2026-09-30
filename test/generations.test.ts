// P16-05: generation transitions, the retained readable generation, the
// model-space guard and the deletion purge, on real PostgreSQL 17 and a real
// Qdrant 1.19.0.
import { createHash } from "node:crypto";
import { QdrantClient } from "@qdrant/js-client-rest";
import { Registry } from "prom-client";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { Indexer } from "../src/application/indexer.js";
import { Retrieval } from "../src/application/retrieval.js";
import { Snapshots } from "../src/application/snapshots.js";
import { Sources } from "../src/application/sources.js";
import type { Tasks } from "../src/application/tasks.js";
import { collectionOf, indexTaskIdOf } from "../src/domain/index.js";
import type { Scope } from "../src/domain/source.js";
import { silentLogger } from "../src/log.js";
import { Metrics } from "../src/metrics.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";
import { apiKey, HashInference, indexSeeded, qdrantIndex, seedParsed, space, startQdrant } from "./rag.js";

const clock = new FakeClock(new Date("2026-09-28T12:00:00Z"));
const objects = new MemoryObjects();
const inference = new HashInference();
const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
let inst: Instance;
let qdrant: { container: StartedTestContainer; url: string };
let raw: QdrantClient;
let tasks: Tasks;
let store: Store;
let indexer: Indexer;
let snapshots: Snapshots;
let retrieval: Retrieval;
let sources: Sources;
let seq = 0;

const sha = (s: string) => `sha256:${createHash("sha256").update(s).digest("hex")}`;
const cmd = () => ({
	tenantId: "tenant_a",
	commandId: `g-cmd-${++seq}`,
	actorId: "alice",
	requestDigest: sha(`${seq}`),
});

const search = (snapshotId: string, query: string) =>
	retrieval.search({
		scope: alice,
		snapshotId,
		query,
		maxContextItems: 8,
		retrievalProfileId: "hybrid-bge-m3-dev-v1",
		deadline: new Date(clock.now().getTime() + 30_000),
	});

async function state(generation: number): Promise<string | undefined> {
	return (await inst.admin("SELECT state FROM index_generations WHERE generation = $1", [generation])).rows[0]?.state;
}

async function points(generation: number, sourceId: string): Promise<number> {
	return (
		await raw.count(collectionOf(generation), {
			filter: { must: [{ key: "source_id", match: { value: sourceId } }] },
			exact: true,
		})
	).count;
}

/** Claims and completes every open index entry of a generation. */
async function drain(generation: number): Promise<void> {
	const open = await inst.admin(
		"SELECT source_id FROM index_entries WHERE generation = $1 AND state IN ('pending', 'running')",
		[generation],
	);
	for (const r of open.rows) {
		const taskId = indexTaskIdOf(r.source_id, 1, generation);
		const workerId = `drain-${++seq}`;
		const c = await tasks.claim(taskId, 1, workerId, 600_000);
		for (;;) {
			const a = await indexer.advance(taskId, 1, workerId, c.task.inputDigest);
			if (a.state === "running") continue;
			if (a.state === "failed") throw new Error(a.failureCode);
			await tasks.submit(taskId, 1, {
				workerId,
				inputDigest: c.task.inputDigest,
				succeeded: true,
				resultRef: a.resultRef,
				resultDigest: a.resultDigest,
				failureCode: "",
			});
			break;
		}
	}
}

beforeAll(async () => {
	[inst, qdrant] = await Promise.all([startInstance(), startQdrant()]);
	raw = new QdrantClient({ url: qdrant.url, apiKey, checkCompatibility: false });
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	const metrics = new Metrics(new Registry());
	const vectors = qdrantIndex(qdrant.url);
	indexer = new Indexer(
		store,
		tasks,
		() => vectors,
		() => inference,
		() => objects,
		() => space,
		() => ({ batchSize: 8, maxResultBytes: 1 << 20, pollMs: 0 }),
		clock,
		silentLogger,
		metrics,
	);
	tasks.setRecords("knowledge-project", indexer);
	snapshots = new Snapshots(store, silentLogger);
	retrieval = new Retrieval(
		store,
		() => vectors,
		() => inference,
		() => objects,
		() => space,
		() => ({
			profileId: "hybrid-bge-m3-dev-v1",
			denseLimit: 20,
			sparseLimit: 20,
			rrfK: 60,
			fusedLimit: 20,
			rerankLimit: 10,
			maxContextChars: 4000,
			maxAllowedSources: 8,
			minRerankScore: 0,
			maxDeadlineMs: 60_000,
		}),
		() => 1 << 20,
		clock,
		silentLogger,
		metrics,
	);
	sources = new Sources(
		store,
		tasks,
		() => objects,
		() => undefined,
		() => ({ maxSourceBytes: 1 << 20 }),
		clock,
		silentLogger,
	);
	for (let i = 0; i < 3; i++) await indexer.reconcile();
	const d = { indexer, tasks, store, clock };
	for (const [id, text] of <[string, string][]>[
		["src-1", "Hero banners use teal gradients."],
		["src-2", "Footers list legal links in slate."],
	])
		await indexSeeded(
			d,
			await seedParsed(inst, objects, { sourceId: id, tenantId: "tenant_a", texts: [text] }),
			"tenant_a",
		);
}, 240_000);

afterAll(async () => {
	await store?.pool.end();
	await inst?.stop();
	await qdrant?.container.stop();
});

describe("generation transitions", () => {
	it("builds a new generation beside the accepted one and accepts it only after every eligible revision is materialized", async () => {
		const old = (await snapshots.create(cmd(), alice, ["src-1", "src-2"])).snapshot;
		expect(old.generation).toBe(1);
		expect(await indexer.createGeneration()).toBe(2);
		await indexer.reconcile(); // collection, backfill of both revisions
		expect(await state(2)).toBe("building");
		const entries = await inst.admin("SELECT count(*)::int AS n FROM index_entries WHERE generation = 2");
		expect(entries.rows[0]?.n).toBe(2);
		// Partly indexed: not qualified, the alias stays on generation 1.
		const one = indexTaskIdOf("src-1", 1, 2);
		const w = "partial-worker";
		const c = await tasks.claim(one, 1, w, 600_000);
		while ((await indexer.advance(one, 1, w, c.task.inputDigest)).state === "running");
		await indexer.reconcile();
		expect(await state(2)).toBe("building");
		expect((await raw.getAliases()).aliases[0]?.collection_name).toBe(collectionOf(1));
		await inst.admin("UPDATE index_entries SET state = 'pending' WHERE task_id = $1", [one]);
		clock.advance(3_600_000); // the partial claim's lease runs out; the entry is claimed again
		await tasks.sweepExpired(10);
		clock.advance(10_000);
		await drain(2);
		await indexer.reconcile(); // materialized
		expect(await state(2)).toBe("materialized");
		await indexer.reconcile(); // accepted, alias moved, generation 1 retired
		expect(await state(2)).toBe("accepted");
		expect(await state(1)).toBe("retired");
		expect((await raw.getAliases()).aliases).toEqual([
			{ alias_name: "anvilkit-knowledge", collection_name: collectionOf(2) },
		]);
		// The old snapshot keeps reading its own, retained generation; a new one reads generation 2.
		expect((await search(old.snapshotId, "teal hero banners")).indexGeneration).toBe(1);
		expect((await search(old.snapshotId, "teal hero banners")).items[0]?.citation.sourceId).toBe("src-1");
		const fresh = (await snapshots.create(cmd(), alice, ["src-1", "src-2"])).snapshot;
		expect(fresh.generation).toBe(2);
		expect((await search(fresh.snapshotId, "teal hero banners")).items[0]?.citation.sourceId).toBe("src-1");
	});

	it("applies deletion to every readable generation: readability ends at once, points are purged from each collection", async () => {
		const old = await inst.admin("SELECT snapshot_id FROM snapshots WHERE generation = 1 LIMIT 1");
		const before = (await search(old.rows[0]?.snapshot_id, "footers legal links slate")).items.map(
			(i) => i.citation.sourceId,
		);
		expect(before).toContain("src-2");
		expect(await points(1, "src-2")).toBe(1);
		expect(await points(2, "src-2")).toBe(1);
		await sources.delete(cmd(), alice, "src-2", 1);
		// Readability ended in the deletion transaction, before any projection was touched.
		const after = await search(old.rows[0]?.snapshot_id, "footers legal links slate");
		expect(after.items.map((i) => i.citation.sourceId)).not.toContain("src-2");
		expect(JSON.stringify(after)).not.toMatch(/Footers/);
		const e = await inst.admin("SELECT DISTINCT state FROM index_entries WHERE source_id = 'src-2'");
		expect(e.rows.map((r) => r.state)).toEqual(["stale"]);
		await indexer.reconcile();
		expect(await points(1, "src-2")).toBe(0);
		expect(await points(2, "src-2")).toBe(0);
		const p = await inst.admin("SELECT points_purged_at FROM sources WHERE source_id = 'src-2'");
		expect(p.rows[0]?.points_purged_at).not.toBeNull();
		// The accepted generation stays qualified without the deleted source.
		await indexer.reconcile();
		expect(await state(2)).toBe("accepted");
	});

	it("never builds a generation for another model's space into an existing one", async () => {
		const other = { ...space, modelRevision: "another-model-revision" };
		const spaced = new Indexer(
			store,
			tasks,
			() => qdrantIndex(qdrant.url),
			() => inference,
			() => objects,
			() => other,
			() => ({ batchSize: 8, maxResultBytes: 1 << 20, pollMs: 0 }),
			clock,
			silentLogger,
			new Metrics(new Registry()),
		);
		await spaced.reconcile();
		const g = await inst.admin(
			"SELECT generation, model_revision, state FROM index_generations WHERE model_revision = 'another-model-revision'",
		);
		expect(g.rows).toEqual([{ generation: "3", model_revision: "another-model-revision", state: "building" }]);
		// Its entries are written only by a builder computing that space; this one refuses them.
		const taskId = indexTaskIdOf("src-1", 1, 3);
		await spaced.reconcile();
		const w = "space-worker";
		const c = await tasks.claim(taskId, 1, w, 600_000);
		expect(await indexer.advance(taskId, 1, w, c.task.inputDigest)).toEqual({
			state: "failed",
			failureCode: "PROFILE_UNQUALIFIED",
		});
	});
});
