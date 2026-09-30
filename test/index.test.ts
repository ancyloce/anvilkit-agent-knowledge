// P16-02: the Index Builder on real PostgreSQL 17 and a real Qdrant 1.19.0.
import { QdrantClient } from "@qdrant/js-client-rest";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { type Point, type QdrantIndex, VectorUnavailable } from "../src/adapters/qdrant.js";
import { type IndexAnswer, Indexer } from "../src/application/indexer.js";
import type { Tasks } from "../src/application/tasks.js";
import { collectionOf, IndexError, indexTaskIdOf, pointIdOf, sourceKeyOf } from "../src/domain/index.js";
import { silentLogger } from "../src/log.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";
import { apiKey, HashInference, qdrantIndex, seedParsed, space, startQdrant } from "./rag.js";

/** The real adapter with scripted faults: a write that lands partly and is never answered. */
class Faulty {
	partialNext = false;
	constructor(readonly real: QdrantIndex) {}
	wrap(): QdrantIndex {
		return new Proxy(this.real, {
			get: (target, prop, recv) => {
				if (prop === "upsert")
					return async (collection: string, points: Point[]) => {
						if (this.partialNext) {
							this.partialNext = false;
							await target.upsert(collection, points.slice(0, Math.ceil(points.length / 2)));
							throw new VectorUnavailable("upsert: socket hang up");
						}
						return target.upsert(collection, points);
					};
				return Reflect.get(target, prop, recv);
			},
		});
	}
}

const clock = new FakeClock(new Date("2026-09-28T12:00:00Z"));
const objects = new MemoryObjects();
const inference = new HashInference();
let inst: Instance;
let qdrant: { container: StartedTestContainer; url: string };
let raw: QdrantClient;
let faulty: Faulty;
let tasks: Tasks;
let store: Store;
let indexer: Indexer;
let activeSpace = space;
let seq = 0;

async function claim(taskId: string): Promise<{ workerId: string; inputDigest: string }> {
	const workerId = `w-${++seq}`;
	const out = await tasks.claim(taskId, 1, workerId, 600_000);
	return { workerId, inputDigest: out.task.inputDigest };
}

async function drive(taskId: string, c: { workerId: string; inputDigest: string }, max = 50): Promise<IndexAnswer> {
	for (let i = 0; i < max; i++) {
		const a = await indexer.advance(taskId, 1, c.workerId, c.inputDigest);
		if (a.state !== "running") return a;
	}
	throw new Error("index step did not settle");
}

async function reconcileUntil(state: string, generation = 1): Promise<void> {
	for (let i = 0; i < 5; i++) {
		await indexer.reconcile();
		const r = await inst.admin("SELECT state FROM index_generations WHERE generation = $1", [generation]);
		if (r.rows[0]?.state === state) return;
	}
	throw new Error(`generation ${generation} did not reach ${state}`);
}

beforeAll(async () => {
	[inst, qdrant] = await Promise.all([startInstance(), startQdrant()]);
	raw = new QdrantClient({ url: qdrant.url, apiKey, checkCompatibility: false });
	faulty = new Faulty(qdrantIndex(qdrant.url));
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	const registry = await import("prom-client").then((m) => new m.Registry());
	const { Metrics } = await import("../src/metrics.js");
	indexer = new Indexer(
		store,
		tasks,
		() => faulty.wrap(),
		() => inference,
		() => objects,
		() => activeSpace,
		() => ({ batchSize: 2, maxResultBytes: 1 << 20, pollMs: 0 }),
		clock,
		silentLogger,
		new Metrics(registry),
	);
	tasks.setRecords("knowledge-project", indexer);
}, 180_000);

afterAll(async () => {
	await store?.pool.end();
	await inst?.stop();
	await qdrant?.container.stop();
});

describe("index generations", () => {
	it("creates generation 1 with named dense/sparse vectors and keyword payload indexes, then accepts it and moves the alias", async () => {
		await reconcileUntil("accepted");
		const info = await raw.getCollection(collectionOf(1));
		const vectors = info.config.params.vectors as Record<string, { size: number; distance: string }>;
		expect(vectors.dense).toMatchObject({ size: 1024, distance: "Cosine" });
		expect(Object.keys(info.config.params.sparse_vectors ?? {})).toEqual(["sparse"]);
		expect(Object.keys(info.payload_schema).sort()).toEqual([
			"fact_id",
			"kind",
			"memory_key",
			"project_id",
			"source_id",
			"source_key",
			"tenant_id",
		]);
		for (const f of Object.values(info.payload_schema)) expect(f?.data_type).toBe("keyword");
		expect((await raw.getAliases()).aliases).toEqual([
			{ alias_name: "anvilkit-knowledge", collection_name: collectionOf(1) },
		]);
	});
});

describe("index entries", () => {
	it("writes a source revision in bounded batches under deterministic ids, verifies every point and accepts only the recorded manifest", async () => {
		const s = await seedParsed(inst, objects, {
			sourceId: "src-a",
			tenantId: "tenant_a",
			texts: ["alpha teal", "beta slate", "gamma hero", "delta card", "epsilon grid"],
		});
		await store.inTx((c) =>
			indexer.scheduleIn(c, {
				requestId: s.requestId,
				sourceId: s.sourceId,
				sourceRevision: 1,
				tenantId: "tenant_a",
				chunkerProfile: "docling-hierarchical",
				chunkerRevision: 1,
				chunkCount: 5,
				correlationId: "corr",
			}),
		);
		// A second schedule of the same revision adds nothing.
		expect(
			await store.inTx((c) =>
				indexer.scheduleIn(c, {
					requestId: s.requestId,
					sourceId: s.sourceId,
					sourceRevision: 1,
					tenantId: "tenant_a",
					chunkerProfile: "docling-hierarchical",
					chunkerRevision: 1,
					chunkCount: 5,
					correlationId: "corr",
				}),
			),
		).toBe(0);
		const taskId = indexTaskIdOf("src-a", 1, 1);
		const c = await claim(taskId);
		const embedsBefore = inference.embeds;
		const a = await drive(taskId, c);
		expect(a.state).toBe("materialized");
		expect(inference.embeds - embedsBefore).toBe(3); // 5 chunks in batches of 2
		const points = await raw.retrieve(collectionOf(1), { ids: s.chunkIds.map(pointIdOf), with_payload: true });
		expect(points).toHaveLength(5);
		for (const p of points) {
			expect(p.payload).toMatchObject({ tenant_id: "tenant_a", source_key: sourceKeyOf("src-a", 1), generation: 1 });
			expect(JSON.stringify(p.payload)).not.toMatch(/teal|slate|hero/); // no text in the projection
		}
		if (a.state !== "materialized") throw new Error("unreachable");
		// Another digest is not the recorded manifest: the attempt is consumed.
		const wrong = await tasks.submit(taskId, 1, {
			workerId: c.workerId,
			inputDigest: c.inputDigest,
			succeeded: true,
			resultRef: a.resultRef,
			resultDigest: `sha256:${"0".repeat(64)}`,
			failureCode: "",
		});
		expect(wrong.task.failureCode).toBe("PROFILE_MISMATCH");
		clock.advance(10_000);
		const c2 = await claim(taskId);
		const again = await drive(taskId, c2);
		expect(again).toEqual(a); // a materialized entry answers without writing again
		const ok = await tasks.submit(taskId, 1, {
			workerId: c2.workerId,
			inputDigest: c2.inputDigest,
			succeeded: true,
			resultRef: a.resultRef,
			resultDigest: a.resultDigest,
			failureCode: "",
		});
		expect(ok.accepted).toBe(true);
		const e = await inst.admin("SELECT state, point_count FROM index_entries WHERE task_id = $1", [taskId]);
		expect(e.rows[0]).toEqual({ state: "accepted", point_count: 5 });
		const ing = await inst.admin("SELECT state FROM ingest_requests WHERE request_id = $1", [s.requestId]);
		expect(ing.rows[0]?.state).toBe("indexed");
		const ev = await inst.admin(
			"SELECT payload FROM outbox WHERE convert_from(decode(payload->>'payload', 'base64'), 'UTF8') LIKE '%revision-indexed%'",
		);
		expect(ev.rows).toHaveLength(1);
	});

	it("rewrites a partly written, unanswered batch under the same ids and repairs missing and stray points before recording", async () => {
		const s = await seedParsed(inst, objects, {
			sourceId: "src-b",
			tenantId: "tenant_a",
			texts: ["one", "two", "three", "four"],
		});
		await store.inTx((c) =>
			indexer.scheduleIn(c, {
				requestId: s.requestId,
				sourceId: s.sourceId,
				sourceRevision: 1,
				tenantId: "tenant_a",
				chunkerProfile: "docling-hierarchical",
				chunkerRevision: 1,
				chunkCount: 4,
				correlationId: "corr",
			}),
		);
		const taskId = indexTaskIdOf("src-b", 1, 1);
		const c = await claim(taskId);
		faulty.partialNext = true;
		const first = await indexer.advance(taskId, 1, c.workerId, c.inputDigest);
		expect(first.state).toBe("running");
		const wt = await inst.admin("SELECT written_through FROM index_entries WHERE task_id = $1", [taskId]);
		expect(wt.rows[0]?.written_through).toBe(0); // the watermark never moves on an unconfirmed write
		// Write everything, then remove one point and plant a stray one before verification.
		for (let i = 0; i < 2; i++) await indexer.advance(taskId, 1, c.workerId, c.inputDigest);
		await raw.delete(collectionOf(1), { wait: true, points: [pointIdOf(s.chunkIds[2] as string)] });
		const stray = pointIdOf("chk-stray");
		await raw.upsert(collectionOf(1), {
			wait: true,
			points: [
				{
					id: stray,
					vector: {
						dense: new Array(1024).fill(0).map((_, i) => (i === 1 ? 1 : 0)),
						sparse: { indices: [1], values: [1] },
					},
					payload: { source_key: sourceKeyOf("src-b", 1), tenant_id: "tenant_a" },
				},
			],
		});
		const a = await drive(taskId, c);
		expect(a.state).toBe("materialized");
		expect(
			(
				await raw.count(collectionOf(1), {
					filter: { must: [{ key: "source_key", match: { value: "src-b@1" } }] },
					exact: true,
				})
			).count,
		).toBe(4);
		expect(await raw.retrieve(collectionOf(1), { ids: [stray] })).toHaveLength(0);
	});

	it("refuses stale claimants and inputs, never embeds another model's space and stops for a deleted source", async () => {
		const s = await seedParsed(inst, objects, { sourceId: "src-c", tenantId: "tenant_a", texts: ["x", "y"] });
		await store.inTx((c) =>
			indexer.scheduleIn(c, {
				requestId: s.requestId,
				sourceId: s.sourceId,
				sourceRevision: 1,
				tenantId: "tenant_a",
				chunkerProfile: "docling-hierarchical",
				chunkerRevision: 1,
				chunkCount: 2,
				correlationId: "corr",
			}),
		);
		const taskId = indexTaskIdOf("src-c", 1, 1);
		const c = await claim(taskId);
		await expect(indexer.advance(taskId, 1, "someone-else", c.inputDigest)).rejects.toThrow(IndexError);
		await expect(indexer.advance(taskId, 1, c.workerId, `sha256:${"1".repeat(64)}`)).rejects.toThrow(/input digest/);
		activeSpace = { ...space, modelRevision: "another-revision" };
		const refused = await indexer.advance(taskId, 1, c.workerId, c.inputDigest);
		expect(refused).toEqual({ state: "failed", failureCode: "PROFILE_UNQUALIFIED" });
		activeSpace = space;

		const d = await seedParsed(inst, objects, { sourceId: "src-d", tenantId: "tenant_a", texts: ["z"] });
		await store.inTx((cl) =>
			indexer.scheduleIn(cl, {
				requestId: d.requestId,
				sourceId: d.sourceId,
				sourceRevision: 1,
				tenantId: "tenant_a",
				chunkerProfile: "docling-hierarchical",
				chunkerRevision: 1,
				chunkCount: 1,
				correlationId: "corr",
			}),
		);
		const dt = indexTaskIdOf("src-d", 1, 1);
		const dc = await claim(dt);
		await inst.admin("UPDATE sources SET deleted = true, deleted_at = now() WHERE source_id = 'src-d'");
		expect(await indexer.advance(dt, 1, dc.workerId, dc.inputDigest)).toEqual({
			state: "failed",
			failureCode: "AUTHORIZATION_REVOKED",
		});
		await inst.admin("UPDATE sources SET deleted = false, deleted_at = NULL WHERE source_id = 'src-d'");
	});

	it("keeps the watermark on a retryable compute outage and fails the entry on a refused computation", async () => {
		const s = await seedParsed(inst, objects, { sourceId: "src-e", tenantId: "tenant_a", texts: ["p", "q"] });
		await store.inTx((c) =>
			indexer.scheduleIn(c, {
				requestId: s.requestId,
				sourceId: s.sourceId,
				sourceRevision: 1,
				tenantId: "tenant_a",
				chunkerProfile: "docling-hierarchical",
				chunkerRevision: 1,
				chunkCount: 2,
				correlationId: "corr",
			}),
		);
		const taskId = indexTaskIdOf("src-e", 1, 1);
		const c = await claim(taskId);
		inference.failNext = "retryable";
		expect((await indexer.advance(taskId, 1, c.workerId, c.inputDigest)).state).toBe("running");
		inference.failNext = "refused";
		expect(await indexer.advance(taskId, 1, c.workerId, c.inputDigest)).toEqual({
			state: "failed",
			failureCode: "COMPUTE_REFUSED",
		});
	});
});
