// Configuration generations at runtime (DD-09 §4): a candidate is loaded
// from the same inputs as the first generation, its pool is constructed
// off-path and probed, and only then does the swap make it the active
// generation; the previous generation's pools drain within one limit
// (retire). A rejected candidate leaves nothing behind and the active
// generation untouched. New admissions read the active generation;
// requests and leases keep what they froze.
import { readFileSync } from "node:fs";
import type pg from "pg";
import { InferenceClient, type InferenceTransport, type Placement } from "./adapters/inference.js";
import { admitParserProfile, type ParserProfile, parserProfile } from "./adapters/jobcontract.js";
import { type Kube, KubeClient, loadKubeConfig } from "./adapters/kube.js";
import { type MemoryStorePort, PostgresMemoryStore } from "./adapters/memorystore.js";
import { type ObjectStore, S3Objects } from "./adapters/objects.js";
import { newPool, type Store } from "./adapters/postgres.js";
import { QdrantIndex, type VectorIndex } from "./adapters/qdrant.js";
import { type RemovalInventory, S3RemovalInventory } from "./adapters/removals.js";
import type { Tasks } from "./application/tasks.js";
import { type ClientTls, type Generation, inputsOf, loadFrom } from "./config.js";
import type { Logger } from "./log.js";
import type { Metrics } from "./metrics.js";
import { IdentityWatcher, loadMaterial } from "./transport/identity.js";

/**
 * The Inference transport under inference.tls: development is the
 * plaintext http:// of the foundation, tls verifies the server against the
 * bundle of ca_file by server_name (the URL host by default), mtls also
 * presents the workload certificate from the watcher (or the named files).
 */
function inferenceTransport(tls: ClientTls, identity?: IdentityWatcher): InferenceTransport {
	switch (tls.mode) {
		case "development":
			return { mode: "development" };
		case "tls":
			return { mode: "tls", ca: () => readFileSync(tls.caFile), serverName: tls.serverName };
		case "mtls":
			if (tls.certFile && tls.keyFile) {
				return {
					mode: "mtls",
					ca: () => readFileSync(tls.caFile),
					identity: () => {
						const m = loadMaterial({ certFile: tls.certFile, keyFile: tls.keyFile, caFile: tls.caFile });
						return { cert: m.certPem, key: m.keyPem };
					},
					serverName: tls.serverName,
				};
			}
			if (!identity) throw new Error("inference.tls.mode mtls needs the workload identity (grpc.identity.mode mtls)");
			return {
				mode: "mtls",
				ca: () => readFileSync(tls.caFile),
				identity: () => ({ cert: identity.current().certPem, key: identity.current().keyPem }),
				serverName: tls.serverName,
			};
	}
}

export interface Runtime {
	gen: Generation;
	pool: pg.Pool;
	/** The workload identity watcher (P0.1); absent under the DEVELOPMENT_ONLY plaintext mode. */
	identity?: IdentityWatcher;
	objects?: ObjectStore;
	kube?: Kube;
	parser?: ParserProfile;
	inference?: InferenceClient;
	/** P2.7: the retained Inference placement of an earlier embedding space (inference.retained), during a model change. */
	retained?: Placement;
	vectors?: VectorIndex;
	/** P17: the PostgresStore projection (its own pool under the Store role). */
	memoryStore?: MemoryStorePort;
	/** P23: the removal inventory in the independent DR store. */
	removals?: RemovalInventory;
	/** P2.7: the cleanup identity's pool (erasure and retention deletes). */
	cleanup?: pg.Pool;
}

/**
 * P2.7: every accepted parse result is readable by the indexer and
 * retrieval (index.max_result_bytes at least the parser profile's
 * maxOutputBytes), or one revision would fail forever at its size.
 */
export function checkResultBound(maxResultBytes: number, parser: ParserProfile | undefined): void {
	if (parser && maxResultBytes < parser.parser.maxOutputBytes)
		throw new Error(
			`index.max_result_bytes ${maxResultBytes} is below the parser profile's maxOutputBytes ${parser.parser.maxOutputBytes}`,
		);
}

/**
 * Constructs and probes the pool of a generation and builds its clients
 * (object store, Kubernetes, reviewed parser profile, Inference, Qdrant,
 * Store, removal inventory, cleanup pool); any failure closes what was built
 * and rejects the generation.
 */
export async function buildRuntime(gen: Generation, previous?: Runtime): Promise<Runtime> {
	// The identity watcher outlives generations: the files are placements
	// and the listener holds the watcher, so a later generation reuses it and
	// retire() leaves it running; the process stops it at its final shutdown.
	// A watcher this build creates belongs to no generation when the build
	// fails, so the failure stops it.
	let identity = previous?.identity;
	let created: IdentityWatcher | undefined;
	const id = gen.config.grpc.identity;
	if (!identity && id.certFile && id.keyFile && id.caFile) {
		created = new IdentityWatcher(
			{ certFile: id.certFile, keyFile: id.keyFile, caFile: id.caFile },
			id.reloadIntervalMs,
			{ warn: (msg, f) => console.error(JSON.stringify({ level: "warn", msg, ...f })) },
		);
		created.start();
		identity = created;
	}
	try {
		return await buildClients(gen, identity);
	} catch (err) {
		created?.stop();
		throw err;
	}
}

async function buildClients(gen: Generation, identity: IdentityWatcher | undefined): Promise<Runtime> {
	const c = gen.config;
	let parser: ParserProfile | undefined;
	let kube: Kube | undefined;
	try {
		parser = c.parser.profile ? admitParserProfile(parserProfile(c.parser.profile), c.development.enabled) : undefined;
		checkResultBound(c.index.maxResultBytes, parser);
		kube = c.parser.profile ? new KubeClient(loadKubeConfig(c.parser.kubeconfig), c.parser.kubeTimeoutMs) : undefined;
	} catch (err) {
		throw new Error(`parser launcher: ${err instanceof Error ? err.message : String(err)}`);
	}
	const objects = c.objects.endpoint
		? new S3Objects({
				endpoint: c.objects.endpoint,
				stageEndpoint: c.objects.stageEndpoint,
				bucket: c.objects.bucket,
				region: c.objects.region,
				accessKeyId: c.objects.accessKeyId,
				secretAccessKey: c.objects.secretAccessKey,
				requestTimeoutMs: c.objects.requestTimeoutMs,
			})
		: undefined;
	const inf = c.inference;
	const inference = inf.url
		? new InferenceClient(
				inf.url,
				inf.timeoutMs,
				{
					embeddingProfileId: inf.embeddingProfile,
					embeddingModelRevision: inf.embeddingRevision,
					dimensions: inf.dimensions,
					rerankProfileId: inf.rerankProfile,
					rerankModelRevision: inf.rerankRevision,
				},
				undefined,
				inferenceTransport(inf.tls, identity),
			)
		: undefined;
	const r = inf.retained;
	// P2.7: the second deployment computes only the earlier embedding space
	// (Knowledge never reranks through it); its answers bind that profile.
	const retained: Placement | undefined =
		inference && r.url
			? {
					space: {
						modelId: "bge-m3",
						modelRevision: r.embeddingRevision,
						embeddingProfile: r.embeddingProfile,
						dimensions: r.dimensions,
						sparseProfile: r.sparseProfile,
					},
					inference: new InferenceClient(
						r.url,
						inf.timeoutMs,
						{
							embeddingProfileId: r.embeddingProfile,
							embeddingModelRevision: r.embeddingRevision,
							dimensions: r.dimensions,
							rerankProfileId: inf.rerankProfile,
							rerankModelRevision: inf.rerankRevision,
						},
						undefined,
						inferenceTransport({ ...inf.tls, serverName: r.serverName }, identity),
					),
				}
			: undefined;
	const q = c.qdrant;
	const vectors = q.url
		? new QdrantIndex({
				url: q.url,
				apiKey: q.apiKey,
				timeoutMs: q.timeoutMs,
				replicationFactor: q.replicationFactor,
				writeConsistencyFactor: q.writeConsistencyFactor,
				writeOrdering: q.writeOrdering,
				readConsistency: q.readConsistency,
			})
		: undefined;
	const memoryStore = c.store.url ? new PostgresMemoryStore(c.store.url, c.store.schema, c.store.maxConn) : undefined;
	const rm = c.removals;
	const removals = rm.endpoint
		? new S3RemovalInventory({
				endpoint: rm.endpoint,
				bucket: rm.bucket,
				region: rm.region,
				accessKeyId: rm.accessKeyId,
				secretAccessKey: rm.secretAccessKey,
				timeoutMs: rm.timeoutMs,
			})
		: undefined;
	const db = gen.config.database;
	const pool = newPool(
		db.url,
		db.maxConn,
		{
			connectTimeoutMs: db.connectTimeoutMs,
			statementTimeoutMs: db.statementTimeoutMs,
			idleTimeoutMs: db.idleTimeoutMs,
			idleInTransactionTimeoutMs: Math.max(db.statementTimeoutMs * 2, 60_000),
		},
		// P2.7: an idle client's failure is reported, the pool replaces it.
		(err) =>
			process.stderr.write(
				`${JSON.stringify({ level: "warn", msg: "database client error", error: err.message.slice(0, 200) })}\n`,
			),
	);
	try {
		const client = await pool.connect();
		try {
			await client.query("SELECT 1");
		} finally {
			client.release();
		}
	} catch (err) {
		await pool.end().catch(() => undefined);
		await memoryStore?.close().catch(() => undefined);
		objects?.close();
		removals?.close();
		kube?.close();
		throw new Error(`database probe: ${err instanceof Error ? err.message : String(err)}`);
	}
	// P2.7: the cleanup identity's own small pool, when placed.
	const cleanup = gen.config.cleanup.url
		? newPool(gen.config.cleanup.url, 2, {
				connectTimeoutMs: db.connectTimeoutMs,
				statementTimeoutMs: db.statementTimeoutMs,
				idleTimeoutMs: db.idleTimeoutMs,
				idleInTransactionTimeoutMs: Math.max(db.statementTimeoutMs * 2, 60_000),
			})
		: undefined;
	return { gen, pool, identity, objects, kube, parser, inference, retained, vectors, memoryStore, removals, cleanup };
}

/**
 * Drains a generation within one limit: the primary pool, the cleanup pool
 * and the Store pool end concurrently under the same deadline, then the
 * other clients close. Reports whether the deadline cut any drain short; the
 * drains still pending at the deadline and every failed drain, a late one
 * included, are logged by name (never a URL), and none rejects.
 */
export async function retire(rt: Runtime, limitMs: number, log?: Pick<Logger, "warn">): Promise<boolean> {
	const pending = new Set<string>();
	const drains: Promise<void>[] = [];
	for (const [name, drain] of [
		["database", () => rt.pool.end()],
		["cleanup", rt.cleanup ? () => (rt.cleanup as pg.Pool).end() : undefined],
		["store", rt.memoryStore ? () => (rt.memoryStore as MemoryStorePort).close() : undefined],
	] as [string, (() => Promise<void>) | undefined][]) {
		if (!drain) continue;
		pending.add(name);
		drains.push(
			Promise.resolve()
				.then(drain)
				.then(
					() => {
						pending.delete(name);
					},
					(err) => {
						pending.delete(name);
						log?.warn("generation drain failed", {
							resource: name,
							error: (err instanceof Error ? err.message : String(err)).slice(0, 200),
						});
					},
				),
		);
	}
	let timer: NodeJS.Timeout | undefined;
	const deadline = new Promise<"deadline">((resolve) => {
		timer = setTimeout(() => resolve("deadline"), limitMs);
		timer.unref();
	});
	const outcome = await Promise.race([Promise.all(drains).then(() => "drained" as const), deadline]);
	clearTimeout(timer);
	const forced = outcome === "deadline" && pending.size > 0;
	if (forced) log?.warn("generation drain cut short by the limit", { resources: [...pending].join(","), limitMs });
	rt.objects?.close();
	rt.kube?.close();
	rt.vectors?.close();
	rt.removals?.close();
	return forced;
}

export class Generations {
	private active: Runtime | null = null;
	private number: number;

	constructor(
		private readonly path: string,
		private readonly environ: NodeJS.ProcessEnv,
		first: Generation,
		private readonly store: Store,
		private readonly tasks: Tasks,
		private readonly metrics: Metrics,
		private readonly log: Logger,
	) {
		this.number = first.number;
	}

	current(): Generation | null {
		return this.active?.gen ?? null;
	}

	/** The active generation's clients; a request keeps the ones it started with. */
	runtime(): Runtime | null {
		return this.active;
	}

	activate(rt: Runtime): Promise<void> {
		const previous = this.active;
		this.active = rt;
		this.tasks.setExpiry(rt.gen.expiresAt);
		this.store.swap(rt.pool);
		const t = rt.gen.config.tasks;
		this.tasks.setBounds({
			maxInputBytes: t.maxInputBytes,
			maxLeaseMs: t.maxLeaseMs,
			retryDelayMs: t.retryDelayMs,
			maxAttempts: t.maxAttempts,
		});
		this.metrics.configGeneration.set(rt.gen.number);
		this.log.info("configuration generation active", {
			generation: rt.gen.number,
			digest: rt.gen.digest,
			secretRevision: rt.gen.secretRevision,
			apolloRelease: rt.gen.apolloRelease,
			profiles: JSON.stringify(rt.gen.profiles),
		});
		if (!previous) return Promise.resolve();
		const started = Date.now();
		return retire(previous, rt.gen.config.reload.drainLimitMs, this.log).then((forced) =>
			this.log.info("previous generation drained", {
				generation: previous.gen.number,
				seconds: (Date.now() - started) / 1000,
				forced,
			}),
		);
	}

	/** Loads a candidate and, when its inputs differ, builds, probes and publishes it. */
	async reload(): Promise<boolean> {
		const active = this.active?.gen;
		let candidate: Generation;
		try {
			candidate = loadFrom(this.path, this.environ, this.number + 1);
		} catch (err) {
			this.metrics.configRejections.inc();
			throw err;
		}
		if (active && inputsOf(candidate) === inputsOf(active)) return false;
		let rt: Runtime;
		try {
			rt = await buildRuntime(candidate, this.active ?? undefined);
		} catch (err) {
			this.metrics.configRejections.inc();
			throw new Error(`generation ${candidate.number} rejected: ${err instanceof Error ? err.message : String(err)}`);
		}
		this.number = candidate.number;
		await this.activate(rt);
		this.metrics.configRotations.inc();
		return true;
	}

	async shutdown(limitMs: number): Promise<boolean> {
		const rt = this.active;
		this.active = null;
		return rt ? retire(rt, limitMs, this.log) : false;
	}
}
