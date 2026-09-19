// Bootstrap of anvilkit-agent-knowledge (DD-09 §3): the first configuration
// generation and its probed pool, metrics, the owner use case, the probe
// listener, the gRPC listener, the lease sweeper and the generation
// watcher; a failed start unwinds what it created. SIGTERM withdraws
// readiness and new admission, drains the server within its bound (forced
// stop afterwards), stops the loops, records the drain and closes the
// generation's pool last, with the probe listener after it.
import { collectDefaultMetrics, Registry } from "prom-client";
import { ControlDispatchQuery } from "./adapters/control.js";
import { Store } from "./adapters/postgres.js";
import { noDispatchQuery, systemClock, Tasks } from "./application/tasks.js";
import { defaultConfigFile, envConfigFile, type Generation, load } from "./config.js";
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
	let ready = false;
	const health = createHealthServer(registry, () => ready && tasks.ready());
	const grpc = createGrpcServer(cfg.grpc.listen, cfg.grpc.capacity, tasks, log);
	let sweeper: NodeJS.Timeout | undefined;
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
				await tasks.observe(cfg.outbox.consumerGroup);
			});
		}, cfg.tasks.sweepIntervalMs);
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
			if (watcher) clearInterval(watcher);
			await Promise.all([sweeping, watching]).catch(() => undefined);
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
