// Shared fixtures of the P17 tests: a real PostgreSQL 17 with the knowledge
// schema and the PostgresStore vendor schema (migrated by the Store
// migration identity), a real Qdrant 1.19.0 node, the deterministic
// Inference double of rag.ts, an accepted index generation of the reviewed
// space, and a runner that claims, advances and submits memory-project
// tasks exactly as the Background Worker does.
import { Registry } from "prom-client";
import type { StartedTestContainer } from "testcontainers";
import * as mdb from "../src/adapters/memorydb.js";
import { type MemoryStorePort, migrateStore, PostgresMemoryStore } from "../src/adapters/memorystore.js";
import { MemoryObjects } from "../src/adapters/objects.js";
import * as db from "../src/adapters/postgres.js";
import type { QdrantIndex } from "../src/adapters/qdrant.js";
import { Indexer } from "../src/application/indexer.js";
import { Memory, type MemoryBounds } from "../src/application/memory.js";
import { MemoryProjector, type ProjectionAnswer } from "../src/application/projection.js";
import type { Tasks } from "../src/application/tasks.js";
import type { SpaceProfile } from "../src/domain/index.js";
import type { Command, Scope } from "../src/domain/source.js";
import { TaskError } from "../src/domain/task.js";
import { silentLogger } from "../src/log.js";
import { Metrics } from "../src/metrics.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";
import { HashInference, qdrantIndex, space, startQdrant } from "./rag.js";

export interface Kit {
	inst: Instance;
	qdrant: { container: StartedTestContainer; url: string };
	clock: FakeClock;
	tasks: Tasks;
	store: db.Store;
	objects: MemoryObjects;
	inference: HashInference;
	vectors: QdrantIndex;
	memoryStore: MemoryStorePort;
	/** Replaces the Store the projector writes to (fault injection); undefined unplaces it. */
	setMemoryStore(s: MemoryStorePort | undefined): void;
	setVectors(v: QdrantIndex | undefined): void;
	memory: Memory;
	projector: MemoryProjector;
	indexer: Indexer;
	metrics: Metrics;
	bounds: MemoryBounds;
	activeSpace: { current: SpaceProfile };
	cmd(scope: Scope): Command;
	stop(): Promise<void>;
}

let seq = 0;

export async function startKit(): Promise<Kit> {
	const [inst, qdrant] = await Promise.all([startInstance(), startQdrant()]);
	await migrateStore(inst.storeMigratorUrl, "memory_store", "anvilkit_knowledge_store");
	const clock = new FakeClock(new Date("2026-09-29T12:00:00Z"));
	const { tasks, store } = newTasks(inst, clock, new FakeDispatch());
	const metrics = new Metrics(new Registry());
	const objects = new MemoryObjects();
	const inference = new HashInference();
	const realVectors = qdrantIndex(qdrant.url);
	let vectors: QdrantIndex | undefined = realVectors;
	const realStore = new PostgresMemoryStore(inst.storeUrl, "memory_store", 2);
	let memoryStore: MemoryStorePort | undefined = realStore;
	const bounds: MemoryBounds = { maxAllowedFacts: 64, maxProjectionEpochs: 3, retryDelayMs: 1000 };
	const activeSpace = { current: space };
	const indexer = new Indexer(
		store,
		tasks,
		() => vectors,
		() => inference,
		() => objects,
		() => activeSpace.current,
		() => ({ batchSize: 8, maxResultBytes: 1 << 20, pollMs: 0 }),
		clock,
		silentLogger,
		metrics,
	);
	const memory = new Memory(
		store,
		tasks,
		() => activeSpace.current,
		() => bounds,
		clock,
		silentLogger,
		metrics,
	);
	const projector = new MemoryProjector(
		store,
		tasks,
		() => memoryStore,
		() => vectors,
		() => inference,
		() => activeSpace.current,
		() => bounds,
		clock,
		silentLogger,
		metrics,
	);
	tasks.setRecords("knowledge-project", indexer);
	tasks.setRecords("memory-project", projector);
	indexer.setMemoryGate((g, v) => projector.generationReady(g, v));
	return {
		inst,
		qdrant,
		clock,
		tasks,
		store,
		objects,
		inference,
		vectors: realVectors,
		memoryStore: realStore,
		setMemoryStore: (s) => {
			memoryStore = s;
		},
		setVectors: (v) => {
			vectors = v;
		},
		memory,
		projector,
		indexer,
		metrics,
		bounds,
		activeSpace,
		cmd: (scope) => ({
			tenantId: scope.tenantId,
			commandId: `kit-cmd-${++seq}`,
			actorId: scope.actorId,
			requestDigest: `sha256:${"c".repeat(64)}`,
		}),
		stop: async () => {
			await realStore.close().catch(() => undefined);
			await store.pool.end();
			await Promise.all([inst.stop(), qdrant.container.stop()]);
		},
	};
}

/** Two reconciliation passes: the empty generation of the space is created, materialized and accepted. */
export async function acceptGeneration(k: Kit): Promise<void> {
	for (let i = 0; i < 4; i++) await k.indexer.reconcile();
}

let workers = 0;

export interface Ran {
	answer: ProjectionAnswer;
	accepted: boolean;
}

/** Claims the latest generation of a memory-project task, advances it to an answer and submits like the Worker. */
export async function runProjection(k: Kit, taskId: string, beforeSubmit?: () => Promise<void>): Promise<Ran> {
	const latest = await db.getLatestRequest(k.store.pool, taskId);
	if (!latest) throw new Error(`no request ${taskId}`);
	const workerId = `kit-worker-${++workers}`;
	const claimed = await k.tasks.claim(taskId, latest.generation, workerId, 600_000);
	let answer: ProjectionAnswer | undefined;
	for (let i = 0; i < 50; i++) {
		answer = await k.projector.advance(taskId, latest.generation, workerId, claimed.task.inputDigest);
		if (answer.state !== "running") break;
	}
	if (!answer || answer.state === "running") throw new Error(`${taskId} did not settle`);
	await beforeSubmit?.();
	try {
		const d = await k.tasks.submit(taskId, latest.generation, {
			workerId,
			inputDigest: claimed.task.inputDigest,
			succeeded: answer.state === "materialized",
			resultRef: answer.state === "materialized" ? answer.resultRef : "",
			resultDigest: answer.state === "materialized" ? answer.resultDigest : "",
			failureCode: answer.state === "failed" ? answer.failureCode : "",
		});
		return { answer, accepted: d.accepted };
	} catch (err) {
		// A fenced claimant's submission is refused, as the Worker sees it.
		if (err instanceof TaskError && err.code === "STALE_EXECUTION") return { answer, accepted: false };
		throw err;
	}
}

/** Runs every open memory-project task until none is pending (the Worker's steady state). */
export async function drainProjections(k: Kit): Promise<number> {
	let ran = 0;
	for (let round = 0; round < 20; round++) {
		const r = await k.inst.admin(
			"SELECT DISTINCT task_id FROM background_requests WHERE task_kind = 'memory-project' AND state = 'pending' ORDER BY task_id",
		);
		if (r.rows.length === 0) return ran;
		for (const x of r.rows) {
			await runProjection(k, x.task_id as string);
			ran++;
		}
	}
	throw new Error("projections did not drain");
}

export const ledger = (k: Kit, factId: string) => mdb.projectionsOf(k.store.pool, factId);
