// P2.7 (AC5): index resilience on real PostgreSQL 17 and Qdrant 1.19.0 — a
// store that does not answer is asked again, a failed entry is attempted
// again as a new task generation and then quarantined, and a quarantined
// revision no longer holds a generation back.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { type IndexAnswer, Indexer, type IndexSettings } from "../src/application/indexer.js";
import type { Tasks } from "../src/application/tasks.js";
import { indexTaskIdOf, qualifies } from "../src/domain/index.js";
import { silentLogger } from "../src/log.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";
import { HashInference, qdrantIndex, seedParsed, space, startQdrant } from "./rag.js";

/** A store whose next reads fail as an unanswered request would. */
class FlakyObjects extends MemoryObjects {
	failNext = 0;
	override async read(key: string, maxBytes: number): Promise<Buffer> {
		if (this.failNext > 0) {
			this.failNext--;
			throw new Error("read ECONNRESET");
		}
		return super.read(key, maxBytes);
	}
}

const clock = new FakeClock(new Date("2026-10-13T12:00:00Z"));
const objects = new FlakyObjects();
let inst: Instance;
let qdrant: Awaited<ReturnType<typeof startQdrant>>;
let tasks: Tasks;
let store: Store;
let indexer: Indexer;
const settings: IndexSettings = { batchSize: 2, maxResultBytes: 1 << 20, pollMs: 0, maxAttempts: 2, retryDelayMs: 0 };
let seq = 0;

async function drive(taskId: string, generation: number, max = 50): Promise<IndexAnswer> {
	const workerId = `w-${++seq}`;
	const claimed = await tasks.claim(taskId, generation, workerId, 600_000);
	for (let i = 0; i < max; i++) {
		const a = await indexer.advance(taskId, generation, workerId, claimed.task.inputDigest);
		if (a.state !== "running") return a;
	}
	throw new Error("index step did not settle");
}

async function schedule(sourceId: string, texts: string[]): Promise<string> {
	const s = await seedParsed(inst, objects, { sourceId, tenantId: "tenant_a", texts });
	await store.inTx((c) =>
		indexer.scheduleIn(c, {
			requestId: s.requestId,
			sourceId,
			sourceRevision: 1,
			tenantId: "tenant_a",
			chunkerProfile: "docling-hierarchical",
			chunkerRevision: 1,
			chunkCount: texts.length,
			correlationId: "corr",
		}),
	);
	return indexTaskIdOf(sourceId, 1, 1);
}

beforeAll(async () => {
	[inst, qdrant] = await Promise.all([startInstance(), startQdrant()]);
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	const registry = await import("prom-client").then((m) => new m.Registry());
	const { Metrics } = await import("../src/metrics.js");
	indexer = new Indexer(
		store,
		tasks,
		() => qdrantIndex(qdrant.url),
		() => new HashInference(),
		() => objects,
		() => space,
		() => settings,
		clock,
		silentLogger,
		new Metrics(registry),
	);
	tasks.setRecords("knowledge-project", indexer);
	for (let i = 0; i < 5; i++) await indexer.reconcile();
}, 180_000);

afterAll(async () => {
	await store?.pool.end();
	await inst?.stop();
	await qdrant?.container.stop();
});

describe("index resilience (P2.7)", () => {
	it("a store that does not answer is asked again; the entry completes instead of failing", async () => {
		const taskId = await schedule("src-flaky", ["one teal", "two slate", "three hero"]);
		objects.failNext = 2;
		const answer = await drive(taskId, 1);
		expect(answer.state).toBe("materialized");
		const r = await inst.admin("SELECT state, attempts FROM index_entries WHERE task_id = $1", [taskId]);
		expect(r.rows[0]).toMatchObject({ state: "materialized", attempts: 1 });
	});

	it("a failed entry is attempted again as a new task generation, then quarantined", async () => {
		const taskId = await schedule("src-broken", ["lost text one", "lost text two"]);
		const ref = await inst.admin("SELECT content_ref FROM chunks WHERE source_id = 'src-broken' LIMIT 1");
		const key = String(ref.rows[0]?.content_ref).split("#")[0] as string;
		objects.objects.delete(key);
		expect(await drive(taskId, 1)).toEqual({ state: "failed", failureCode: "CHUNK_UNVERIFIED" });
		clock.advance(1_000);
		const g = (await inst.admin("SELECT * FROM index_generations WHERE generation = 1")).rows[0];
		expect(g).toBeDefined();
		await indexer.retryFailed({
			generation: 1,
			chunkerProfile: "docling-hierarchical",
			chunkerRevision: 1,
			embeddingProfile: space.embeddingProfile,
			modelRevision: space.modelRevision,
		} as Parameters<Indexer["retryFailed"]>[0]);
		let e = await inst.admin("SELECT state, attempts, failure_code FROM index_entries WHERE task_id = $1", [taskId]);
		expect(e.rows[0]).toMatchObject({ state: "pending", attempts: 2, failure_code: null });
		const gens = await inst.admin(
			"SELECT generation::int AS g FROM background_requests WHERE task_id = $1 ORDER BY generation",
			[taskId],
		);
		expect(gens.rows.map((x) => x.g)).toEqual([1, 2]);
		expect(await drive(taskId, 2)).toEqual({ state: "failed", failureCode: "CHUNK_UNVERIFIED" });
		clock.advance(1_000);
		await indexer.retryFailed({
			generation: 1,
			chunkerProfile: "docling-hierarchical",
			chunkerRevision: 1,
			embeddingProfile: space.embeddingProfile,
			modelRevision: space.modelRevision,
		} as Parameters<Indexer["retryFailed"]>[0]);
		e = await inst.admin("SELECT state, attempts, failure_code FROM index_entries WHERE task_id = $1", [taskId]);
		expect(e.rows[0]).toMatchObject({ state: "quarantined", attempts: 2, failure_code: "CHUNK_UNVERIFIED" });
	});

	it("quarantined revisions settle a generation; open or failed ones still hold it", () => {
		expect(
			qualifies({ eligible: 3, accepted: 2, quarantined: 1, open: 0, expectedPoints: 5, countedPoints: 5 }),
		).toEqual({ ok: true });
		expect(
			qualifies({ eligible: 3, accepted: 2, quarantined: 0, open: 0, expectedPoints: 5, countedPoints: 5 }).ok,
		).toBe(false);
		expect(
			qualifies({ eligible: 3, accepted: 1, quarantined: 1, open: 1, expectedPoints: 5, countedPoints: 5 }).ok,
		).toBe(false);
	});
});

describe("result bound (P2.7)", () => {
	it("refuses an index result bound below the parser profile's output bound", async () => {
		const { checkResultBound } = await import("../src/generations.js");
		const { parserProfile } = await import("../src/adapters/jobcontract.js");
		const profile = parserProfile("parser-docling-dev-v1");
		expect(() => checkResultBound(profile.parser.maxOutputBytes - 1, profile)).toThrow(/below the parser profile/);
		expect(() => checkResultBound(profile.parser.maxOutputBytes, profile)).not.toThrow();
		expect(() => checkResultBound(1024, undefined)).not.toThrow();
	});
});

describe("count check and document rebuild (P2.7 AC7)", () => {
	it("detects a deleted accepted collection within one check, surfaces it, and a rebuild writes the points again", async () => {
		const taskId = await schedule("src-count", ["count one", "count two"]);
		expect((await drive(taskId, 1)).state).toBe("materialized");
		// Accept the entry as the lane would (the result accepted for its task).
		await inst.admin("UPDATE index_entries SET state = 'accepted' WHERE task_id = $1", [taskId]);
		const g = (await inst.admin("SELECT * FROM index_generations WHERE state = 'accepted'")).rows[0];
		const gen = {
			generation: Number(g.generation),
			collectionName: g.collection_name,
			chunkerProfile: "docling-hierarchical",
			chunkerRevision: 1,
			embeddingProfile: space.embeddingProfile,
			modelRevision: space.modelRevision,
		} as Parameters<Indexer["checkCount"]>[1];
		const vectors = qdrantIndex(qdrant.url);
		expect(await indexer.checkCount(vectors, gen)).toBe(0);
		expect(indexer.documentsHealthy()).toBe(true);
		// The collection is deleted out of band.
		const { QdrantClient } = await import("@qdrant/js-client-rest");
		const { apiKey } = await import("./rag.js");
		await new QdrantClient({ url: qdrant.url, apiKey, checkCompatibility: false }).deleteCollection(gen.collectionName);
		expect(await indexer.checkCount(vectors, gen)).toBeGreaterThan(0);
		expect(indexer.documentsHealthy()).toBe(false);
		// The rebuild requests every live entry again as a new task generation.
		expect(await indexer.rebuildDocuments(gen.generation)).toBeGreaterThan(0);
		const e = await inst.admin("SELECT state, attempts FROM index_entries WHERE task_id = $1", [taskId]);
		expect(e.rows[0]).toMatchObject({ state: "pending", attempts: 2 });
		await indexer.reconcile(); // ensures the collection again
		expect((await drive(taskId, 2)).state).toBe("materialized");
		await inst.admin("UPDATE index_entries SET state = 'accepted' WHERE task_id = $1", [taskId]);
		expect(await indexer.checkCount(vectors, gen)).toBe(0);
		expect(indexer.documentsHealthy()).toBe(true);
	});
});

describe("chunker change (P2.7 AC6)", () => {
	it("a generation of a new chunker waits for every indexed revision's re-parse, which is requested once", async () => {
		const idx = await import("../src/adapters/indexdb.js");
		const { Sources } = await import("../src/application/sources.js");
		const { collectionOf } = await import("../src/domain/index.js");
		const g2 = await store.inTx((c) => idx.insertGeneration(c, { ...space, chunkerRevision: 2 }, collectionOf));
		const gen = (await idx.getGeneration(store.pool, g2)) as NonNullable<Awaited<ReturnType<typeof idx.getGeneration>>>;
		const before = await idx.progressOf(store.pool, gen);
		expect(before.waiting).toBeGreaterThan(0);
		expect(qualifies({ ...before, countedPoints: before.expectedPoints }).ok).toBe(false);
		const sources = new Sources(
			store,
			tasks,
			() => objects,
			() => ({
				profileId: "parser-docling-dev-v1",
				profileRevision: 2,
				chunkerId: "docling-hierarchical",
				chunkerRevision: 2,
				mediaTypes: ["text/markdown"],
				maxInputBytes: 1 << 20,
			}),
			() => ({ maxSourceBytes: 1 << 20, uploadTtlMs: 900_000, legacyUploads: true }),
			clock,
			silentLogger,
		);
		const requested = await sources.requestReparse(100);
		expect(requested).toBe(before.waiting);
		expect(await sources.requestReparse(100)).toBe(0);
		const tasksOf = await inst.admin("SELECT count(*)::int AS n FROM background_requests WHERE task_id LIKE '%-p2-c2'");
		expect(tasksOf.rows[0].n).toBe(requested);
		// Requested but not parsed yet: still waiting.
		expect((await idx.progressOf(store.pool, gen)).waiting).toBe(before.waiting);
		// Parsed under the new chunker: no longer held back by it.
		await inst.admin("UPDATE ingest_requests SET state = 'indexing', chunk_count = 1 WHERE chunker_revision = 2");
		expect((await idx.progressOf(store.pool, gen)).waiting).toBe(0);
	});
});
