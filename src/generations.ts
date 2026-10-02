// Configuration generations at runtime (DD-09 §4): a candidate is loaded
// from the same inputs as the first generation, its pool is constructed
// off-path and probed, and only then does the swap make it the active
// generation; the previous pool drains within the limit. A rejected
// candidate leaves nothing behind and the active generation untouched. New
// admissions read the active generation; requests and leases keep what
// they froze.
import type pg from "pg";
import { InferenceClient } from "./adapters/inference.js";
import { type ParserProfile, parserProfile } from "./adapters/jobcontract.js";
import { type Kube, KubeClient, loadKubeConfig } from "./adapters/kube.js";
import { type MemoryStorePort, PostgresMemoryStore } from "./adapters/memorystore.js";
import { type ObjectStore, S3Objects } from "./adapters/objects.js";
import { newPool, type Store } from "./adapters/postgres.js";
import { QdrantIndex, type VectorIndex } from "./adapters/qdrant.js";
import { type RemovalInventory, S3RemovalInventory } from "./adapters/removals.js";
import type { Tasks } from "./application/tasks.js";
import { type Generation, inputsOf, loadFrom } from "./config.js";
import type { Logger } from "./log.js";
import type { Metrics } from "./metrics.js";

export interface Runtime {
	gen: Generation;
	pool: pg.Pool;
	objects?: ObjectStore;
	kube?: Kube;
	parser?: ParserProfile;
	inference?: InferenceClient;
	vectors?: VectorIndex;
	/** P17: the PostgresStore projection (its own pool under the Store role). */
	memoryStore?: MemoryStorePort;
	/** P23: the removal inventory in the independent DR store. */
	removals?: RemovalInventory;
}

/**
 * Constructs and probes the pool of a generation and builds its object
 * store, Kubernetes client and reviewed parser profile; any failure closes
 * what was built and rejects the generation.
 */
export async function buildRuntime(gen: Generation): Promise<Runtime> {
	const c = gen.config;
	let parser: ParserProfile | undefined;
	let kube: Kube | undefined;
	try {
		parser = c.parser.profile ? parserProfile(c.parser.profile) : undefined;
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
			})
		: undefined;
	const inf = c.inference;
	const inference = inf.url
		? new InferenceClient(inf.url, inf.timeoutMs, {
				embeddingProfileId: inf.embeddingProfile,
				embeddingModelRevision: inf.embeddingRevision,
				dimensions: inf.dimensions,
				rerankProfileId: inf.rerankProfile,
				rerankModelRevision: inf.rerankRevision,
			})
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
	const pool = newPool(gen.config.database.url, gen.config.database.maxConn);
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
	return { gen, pool, objects, kube, parser, inference, vectors, memoryStore, removals };
}

/** Drains the pool within the limit; reports whether the limit cut it short. */
export async function retire(rt: Runtime, limitMs: number): Promise<boolean> {
	let forced = false;
	const bound = new Promise<void>((resolve) =>
		setTimeout(() => {
			forced = true;
			resolve();
		}, limitMs).unref(),
	);
	await Promise.race([rt.pool.end(), bound]);
	rt.objects?.close();
	rt.kube?.close();
	rt.vectors?.close();
	rt.removals?.close();
	await rt.memoryStore?.close().catch(() => undefined);
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
		return retire(previous, rt.gen.config.reload.drainLimitMs).then((forced) =>
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
			rt = await buildRuntime(candidate);
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
		return rt ? retire(rt, limitMs) : false;
	}
}
