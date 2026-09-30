// Bootstrap of anvilkit-agent-knowledge (DD-09 §3): the first configuration
// generation and its probed pool, metrics, the owner use case, the probe
// listener, the gRPC listener, the lease sweeper and the generation
// watcher; a failed start unwinds what it created. SIGTERM withdraws
// readiness and new admission, drains the server within its bound (forced
// stop afterwards), stops the loops, records the drain and closes the
// generation's pool last, with the probe listener after it.
import { collectDefaultMetrics, Registry } from "prom-client";
import { ControlDispatchQuery } from "./adapters/control.js";
import type { ParserProfile } from "./adapters/jobcontract.js";
import { Store } from "./adapters/postgres.js";
import { Indexer } from "./application/indexer.js";
import { Ingest, planOf } from "./application/ingest.js";
import { Memory } from "./application/memory.js";
import { MemoryProjector } from "./application/projection.js";
import { Recall } from "./application/recall.js";
import { Retrieval } from "./application/retrieval.js";
import { Snapshots } from "./application/snapshots.js";
import { Sources } from "./application/sources.js";
import { noDispatchQuery, systemClock, Tasks } from "./application/tasks.js";
import { defaultConfigFile, envConfigFile, type Generation, load } from "./config.js";
import type { SpaceProfile } from "./domain/index.js";
import { buildRuntime, Generations, retire } from "./generations.js";
import { jsonLogger, type Logger } from "./log.js";
import { Metrics } from "./metrics.js";
import { createGrpcServer } from "./transport/grpc.js";
import { createHealthServer, listen } from "./transport/health.js";

export interface Started {
	stop(): Promise<void>;
	done: Promise<void>;
}

export async function start(
	first: Generation,
	log: Logger = jsonLogger(),
	environ: NodeJS.ProcessEnv = process.env,
): Promise<Started> {
	const cfg = first.config;
	const registry = new Registry();
	collectDefaultMetrics({ register: registry });
	const metrics = new Metrics(registry);
	let rt: Awaited<ReturnType<typeof buildRuntime>>;
	try {
		rt = await buildRuntime(first);
	} catch (err) {
		metrics.configRejections.inc();
		throw new Error(
			`configuration generation ${first.number} rejected: ${err instanceof Error ? err.message : String(err)}`,
		);
	}
	const store = new Store(rt.pool);
	const dispatch = cfg.control.address
		? new ControlDispatchQuery(cfg.control.address, cfg.control.timeoutMs)
		: noDispatchQuery;
	if (!cfg.control.address)
		log.warn("no Control placement: expired external-effect leases are never reassigned (control.address unset)");
	const t = cfg.tasks;
	const tasks = new Tasks(
		store,
		dispatch,
		{
			maxInputBytes: t.maxInputBytes,
			maxLeaseMs: t.maxLeaseMs,
			retryDelayMs: t.retryDelayMs,
			maxAttempts: t.maxAttempts,
		},
		systemClock,
		log,
		metrics,
	);
	const gens = new Generations(environ[envConfigFile] || defaultConfigFile, environ, first, store, tasks, metrics, log);
	const active = () => gens.current()?.config ?? cfg;
	const sources = new Sources(
		store,
		tasks,
		() => gens.runtime()?.objects,
		() => {
			const p = gens.runtime()?.parser;
			return p ? planOf(p) : undefined;
		},
		() => ({ maxSourceBytes: active().sources.maxBytes }),
		systemClock,
		log,
	);
	const ingest = new Ingest(
		store,
		() => gens.runtime()?.kube,
		() => gens.runtime()?.objects,
		() => gens.runtime()?.parser,
		() => {
			const p = active().parser;
			return {
				namespace: p.namespace,
				imageRegistry: p.imageRegistry,
				nodePool: p.nodePool,
				seccompProfile: p.seccompProfile,
				stageSecret: "",
				pollMs: p.pollMs,
				deadlineGraceMs: p.deadlineGraceMs,
				presignTtlSeconds: p.presignTtlSeconds,
			};
		},
		systemClock,
		log,
		metrics,
	);
	tasks.setRecords("knowledge-ingest", ingest);
	const indexer = new Indexer(
		store,
		tasks,
		() => gens.runtime()?.vectors,
		() => gens.runtime()?.inference,
		() => gens.runtime()?.objects,
		() => spaceOf(active(), gens.runtime()?.parser),
		() => {
			const i = active().index;
			return { batchSize: i.batchSize, maxResultBytes: i.maxResultBytes, pollMs: i.pollMs };
		},
		systemClock,
		log,
		metrics,
	);
	tasks.setRecords("knowledge-project", indexer);
	ingest.onIndexable((c, r) => indexer.scheduleIn(c, r));
	if (!cfg.qdrant.url) log.warn("no Qdrant placement: nothing is indexed or retrieved (qdrant.url unset)");
	if (!cfg.parser.profile)
		log.warn("no parser profile: sources are refused and ingest tasks fail (parser.profile unset)");
	let ready = false;
	const health = createHealthServer(registry, () => ready && tasks.ready());
	const snapshots = new Snapshots(store, log);
	const retrieval = new Retrieval(
		store,
		() => gens.runtime()?.vectors,
		() => gens.runtime()?.inference,
		() => gens.runtime()?.objects,
		() => spaceOf(active(), gens.runtime()?.parser),
		() => {
			const r = active().retrieval;
			return r.profileId ? { ...r } : undefined;
		},
		() => active().index.maxResultBytes,
		systemClock,
		log,
		metrics,
	);
	// P17: MemoryFact decisions, their projections and recall. The vector
	// space of memory points is the reviewed embedding profile alone (no
	// chunker); it serves only generations of that space.
	const memory = new Memory(
		store,
		tasks,
		() => memorySpaceOf(active()),
		() => active().memory,
		systemClock,
		log,
		metrics,
	);
	const projector = new MemoryProjector(
		store,
		tasks,
		() => gens.runtime()?.memoryStore,
		() => gens.runtime()?.vectors,
		() => gens.runtime()?.inference,
		() => memorySpaceOf(active()),
		() => active().memory,
		systemClock,
		log,
		metrics,
	);
	tasks.setRecords("memory-project", projector);
	indexer.setMemoryGate((g, v) => projector.generationReady(g, v));
	if (!cfg.store.url) log.warn("no memory Store placement: facts are not projected to the Store (store.url unset)");
	const recall = new Recall(
		store,
		() => gens.runtime()?.vectors,
		() => gens.runtime()?.inference,
		() => memorySpaceOf(active()),
		() => {
			const r = active().retrieval;
			return r.profileId ? { ...r } : undefined;
		},
		() => active().memory,
		systemClock,
		log,
		metrics,
	);
	const grpc = createGrpcServer(
		cfg.grpc.listen,
		cfg.grpc.capacity,
		tasks,
		log,
		sources,
		ingest,
		indexer,
		snapshots,
		retrieval,
		memory,
		projector,
		recall,
	);
	let sweeper: NodeJS.Timeout | undefined;
	let reconciler: NodeJS.Timeout | undefined;
	let reconciling: Promise<void> = Promise.resolve();
	let watcher: NodeJS.Timeout | undefined;
	let sweeping: Promise<void> = Promise.resolve();
	let watching: Promise<void> = Promise.resolve();
	try {
		await listen(health, cfg.health.listen);
		await grpc.listen();
		await gens.activate(rt);
		sweeper = setInterval(() => {
			sweeping = sweeping.then(async () => {
				await tasks.sweepExpired(100).catch((err) => log.warn("lease sweep failed", { error: String(err) }));
				await ingest.reapOrphans(20).catch((err) => log.warn("parser reap failed", { error: String(err) }));
				await tasks.observe(cfg.outbox.consumerGroup);
			});
		}, cfg.tasks.sweepIntervalMs);
		reconciler = setInterval(() => {
			reconciling = reconciling.then(() =>
				indexer
					.reconcile()
					.catch((err) => log.warn("index reconciliation failed", { error: String(err) }))
					.then(() => memory.expireDue(100))
					.then(() => projector.reconcile())
					.catch((err) => log.warn("memory reconciliation failed", { error: String(err) })),
			);
		}, cfg.index.qualifyIntervalMs);
		watcher = setInterval(() => {
			watching = watching.then(() =>
				gens.reload().then(
					() => undefined,
					(err) =>
						log.warn("configuration candidate rejected; the active generation stays", {
							error: err instanceof Error ? err.message : String(err),
						}),
				),
			);
		}, cfg.reload.intervalMs);
		grpc.serve();
		ready = true;
	} catch (err) {
		// Unwind: no listener or loop survives a failed start.
		if (sweeper) clearInterval(sweeper);
		if (reconciler) clearInterval(reconciler);
		if (watcher) clearInterval(watcher);
		await grpc.stop(0).catch(() => undefined);
		health.close();
		await retire(rt, cfg.reload.drainLimitMs);
		if (dispatch instanceof ControlDispatchQuery) dispatch.close();
		throw err;
	}
	log.info("knowledge serving", {
		listen: cfg.grpc.listen,
		healthListen: cfg.health.listen,
		generation: first.number,
		control: cfg.control.address || "(none)",
		parserProfile: cfg.parser.profile || "(none)",
		parserNamespace: cfg.parser.namespace,
	});
	let resolveDone!: () => void;
	const done = new Promise<void>((r) => {
		resolveDone = r;
	});
	let stopping: Promise<void> | undefined;
	const stop = () => {
		if (stopping) return stopping;
		stopping = (async () => {
			const begin = Date.now();
			ready = false;
			grpc.withdraw();
			const forcedServer = await grpc.stop(cfg.grpc.shutdownTimeoutMs);
			if (sweeper) clearInterval(sweeper);
			if (reconciler) clearInterval(reconciler);
			if (watcher) clearInterval(watcher);
			await Promise.all([sweeping, watching, reconciling]).catch(() => undefined);
			const forcedDrain = await gens.shutdown(cfg.reload.drainLimitMs);
			if (dispatch instanceof ControlDispatchQuery) dispatch.close();
			metrics.drainSeconds.set((Date.now() - begin) / 1000);
			if (forcedServer || forcedDrain) metrics.forcedStop.set(1);
			log.info("knowledge stopped", { drainSeconds: (Date.now() - begin) / 1000, forced: forcedServer || forcedDrain });
			await new Promise<void>((r) => health.close(() => r()));
			resolveDone();
		})();
		return stopping;
	};
	return { stop, done };
}

/**
 * The vector space new generations are built for: the reviewed embedding
 * profile of the active configuration, its sparse profile and the chunker of
 * the reviewed parser profile. Without an Inference placement or a parser
 * profile no generation is created.
 */
export function spaceOf(c: Generation["config"], parser: ParserProfile | undefined): SpaceProfile | undefined {
	if (!c.inference.url || !c.qdrant.url || !parser) return undefined;
	return {
		modelId: "bge-m3",
		modelRevision: c.inference.embeddingRevision,
		embeddingProfile: c.inference.embeddingProfile,
		dimensions: c.inference.dimensions,
		sparseProfile: c.index.sparseProfile,
		chunkerProfile: parser.parser.chunker.chunkerId,
		chunkerRevision: Number(parser.parser.chunker.revision),
	};
}

/** The vector space of memory points: the reviewed embedding profile, when Inference and Qdrant are placed. */
export function memorySpaceOf(c: Generation["config"]): SpaceProfile | undefined {
	if (!c.inference.url || !c.qdrant.url) return undefined;
	return {
		modelId: "bge-m3",
		modelRevision: c.inference.embeddingRevision,
		embeddingProfile: c.inference.embeddingProfile,
		dimensions: c.inference.dimensions,
		sparseProfile: c.index.sparseProfile,
		chunkerProfile: "",
		chunkerRevision: 0,
	};
}

const entry = process.argv[1] ?? "";
if (entry.endsWith("main.js") || entry.endsWith("main.ts")) {
	const log = jsonLogger();
	start(load(), log)
		.then((s) => {
			process.once("SIGTERM", () => void s.stop());
			process.once("SIGINT", () => void s.stop());
			return s.done;
		})
		.then(() => process.exit(0))
		.catch((err) => {
			process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`);
			process.exit(1);
		});
}
