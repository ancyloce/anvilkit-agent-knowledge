// Knowledge's immutable configuration generations (DD-09 §4): defaults <
// the reviewed, secret-free config.yaml < a validated, unexpired Apollo
// snapshot (non-secret keys as dotted paths; packages/profile-schemas of
// the parent describes the file) < the allowlisted ANVILKIT_KNOWLEDGE_*
// environment. A candidate is validated as a whole and becomes a complete
// generation or is rejected; nothing starts on a rejected candidate and no
// shared value is mutated field by field. The database URL is the only
// secret: it arrives from the environment or from a mounted secret file
// whose rotation produces a new generation; it is never accepted from the
// file or the snapshot and never logged.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";

export const envPrefix = "ANVILKIT_KNOWLEDGE_";
export const envConfigFile = "ANVILKIT_KNOWLEDGE_CONFIG";
export const defaultConfigFile = "config.yaml";

export interface Config {
	grpc: { listen: string; capacity: number; shutdownTimeoutMs: number };
	health: { listen: string };
	/** Spans over OTLP/HTTP to the collector when placed (none otherwise), sampled at sampleRatio; read at process start. */
	telemetry: { otlpEndpoint: string; sampleRatio: number };
	database: { url: string; urlFile: string; maxConn: number };
	tasks: {
		maxInputBytes: number;
		maxLeaseMs: number;
		retryDelayMs: number;
		maxAttempts: number;
		sweepIntervalMs: number;
	};
	outbox: { consumerGroup: string };
	control: { address: string; timeoutMs: number };
	objects: {
		endpoint: string;
		stageEndpoint: string;
		bucket: string;
		region: string;
		accessKeyId: string;
		secretAccessKey: string;
		credentialsFile: string;
	};
	sources: { maxBytes: number };
	parser: {
		profile: string;
		namespace: string;
		nodePool: string;
		seccompProfile: string;
		pollMs: number;
		deadlineGraceMs: number;
		presignTtlSeconds: number;
		kubeTimeoutMs: number;
		imageRegistry: string;
		kubeconfig: string;
	};
	inference: {
		url: string;
		timeoutMs: number;
		maxBatch: number;
		embeddingProfile: string;
		embeddingRevision: string;
		dimensions: number;
		rerankProfile: string;
		rerankRevision: string;
	};
	qdrant: {
		url: string;
		apiKey: string;
		timeoutMs: number;
		replicationFactor: number;
		writeConsistencyFactor: number;
		writeOrdering: "weak" | "medium" | "strong";
		readConsistency: "all" | "majority" | "quorum";
	};
	index: {
		sparseProfile: string;
		batchSize: number;
		maxResultBytes: number;
		pollMs: number;
		qualifyIntervalMs: number;
	};
	retrieval: {
		profileId: string;
		denseLimit: number;
		sparseLimit: number;
		rrfK: number;
		fusedLimit: number;
		rerankLimit: number;
		maxContextChars: number;
		maxAllowedSources: number;
		minRerankScore: number;
		maxDeadlineMs: number;
	};
	store: { url: string; schema: string; maxConn: number };
	memory: { maxAllowedFacts: number; maxProjectionEpochs: number; retryDelayMs: number };
	/**
	 * The removal inventory in the independent DR store (P23): its own
	 * bucket and bucket-limited credentials; unplaced, deletions and
	 * revocations are not preserved across a database restore.
	 */
	removals: {
		endpoint: string;
		bucket: string;
		region: string;
		accessKeyId: string;
		secretAccessKey: string;
		credentialsFile: string;
		timeoutMs: number;
		windowMarginMs: number;
		reconcileIntervalMs: number;
	};
	apollo: { mode: "disabled" | "snapshot"; snapshotFile: string; appId: string };
	reload: { intervalMs: number; drainLimitMs: number };
}

/** One complete, validated generation and the digests that identify its inputs. */
export interface Generation {
	number: number;
	config: Config;
	digest: string;
	secretRevision: string;
	profiles: Record<string, string>;
	apolloRelease: string;
	expiresAt?: number;
}

export class ConfigError extends Error {}

/** Go-style durations: 500ms, 15s, 5m, 1h (integers, one unit). */
export function parseDuration(text: unknown, key: string): number {
	if (typeof text === "number" && Number.isInteger(text) && text >= 0) return text;
	const m = typeof text === "string" ? /^(\d+)(ms|s|m|h)$/.exec(text) : null;
	if (!m) throw new ConfigError(`${key}: ${JSON.stringify(text)} is not a duration (e.g. 500ms, 15s, 5m)`);
	return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as "ms" | "s" | "m" | "h"];
}

const envOverrides: Record<string, string> = {
	ANVILKIT_KNOWLEDGE_LISTEN: "grpc.listen",
	ANVILKIT_KNOWLEDGE_HEALTH_LISTEN: "health.listen",
	ANVILKIT_KNOWLEDGE_TELEMETRY_OTLP_ENDPOINT: "telemetry.otlp_endpoint",
	ANVILKIT_KNOWLEDGE_DATABASE_URL: "database.url",
	ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE: "database.url_file",
	ANVILKIT_KNOWLEDGE_CONTROL_ADDRESS: "control.address",
	ANVILKIT_KNOWLEDGE_APOLLO_SNAPSHOT_FILE: "apollo.snapshot_file",
	ANVILKIT_KNOWLEDGE_OBJECTS_ENDPOINT: "objects.endpoint",
	ANVILKIT_KNOWLEDGE_OBJECTS_STAGE_ENDPOINT: "objects.stage_endpoint",
	ANVILKIT_KNOWLEDGE_OBJECTS_ACCESS_KEY_ID: "objects.access_key_id",
	ANVILKIT_KNOWLEDGE_OBJECTS_SECRET_ACCESS_KEY: "objects.secret_access_key",
	ANVILKIT_KNOWLEDGE_OBJECTS_CREDENTIALS_FILE: "objects.credentials_file",
	ANVILKIT_KNOWLEDGE_KUBECONFIG: "parser.kubeconfig",
	ANVILKIT_KNOWLEDGE_IMAGE_REGISTRY: "parser.image_registry",
	ANVILKIT_KNOWLEDGE_INFERENCE_URL: "inference.url",
	ANVILKIT_KNOWLEDGE_QDRANT_URL: "qdrant.url",
	ANVILKIT_KNOWLEDGE_QDRANT_API_KEY: "qdrant.api_key",
	ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL: "store.url",
	ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "removals.endpoint",
	ANVILKIT_KNOWLEDGE_REMOVALS_ACCESS_KEY_ID: "removals.access_key_id",
	ANVILKIT_KNOWLEDGE_REMOVALS_SECRET_ACCESS_KEY: "removals.secret_access_key",
	ANVILKIT_KNOWLEDGE_REMOVALS_CREDENTIALS_FILE: "removals.credentials_file",
};

const secretKeys = [
	"database.url",
	"objects.access_key_id",
	"objects.secret_access_key",
	"qdrant.api_key",
	"store.url",
	"removals.access_key_id",
	"removals.secret_access_key",
];
const placementKeys = [
	"telemetry.otlp_endpoint",
	"database.url_file",
	"control.address",
	"apollo.snapshot_file",
	"objects.endpoint",
	"objects.stage_endpoint",
	"objects.credentials_file",
	"parser.kubeconfig",
	"parser.image_registry",
	"inference.url",
	"qdrant.url",
	"removals.endpoint",
	"removals.credentials_file",
];

type Raw = Record<string, unknown>;

function isObject(v: unknown): v is Raw {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

function get(raw: Raw, path: string): unknown {
	let cur: unknown = raw;
	for (const part of path.split(".")) {
		if (!isObject(cur)) return undefined;
		cur = cur[part];
	}
	return cur;
}

function set(raw: Raw, path: string, value: unknown): void {
	const parts = path.split(".");
	let cur = raw;
	for (const part of parts.slice(0, -1)) {
		const next = cur[part];
		if (!isObject(next)) {
			const fresh: Raw = {};
			cur[part] = fresh;
			cur = fresh;
		} else cur = next;
	}
	cur[parts[parts.length - 1] as string] = value;
}

const defaults: Raw = {
	grpc: { listen: "127.0.0.1:9105", capacity: 64, shutdown_timeout: "20s" },
	health: { listen: "127.0.0.1:9115" },
	telemetry: { sample_ratio: 1 },
	database: { max_conn: 8 },
	tasks: { max_input_bytes: 65536, max_lease: "10m", retry_delay: "5s", max_attempts: 3, sweep_interval: "2s" },
	outbox: { consumer_group: "anvilkit-agent-knowledge-forwarder" },
	control: { timeout: "5s" },
	objects: { bucket: "anvilkit-knowledge", region: "us-east-1" },
	sources: { max_bytes: 33554432 },
	parser: {
		profile: "",
		namespace: "anvilkit-parsing",
		node_pool: "parsing",
		seccomp_profile: "anvilkit/candidate.json",
		poll_interval: "1s",
		deadline_grace: "30s",
		presign_ttl: "15m",
		kube_timeout: "10s",
	},
	inference: {
		timeout: "30s",
		max_batch: 32,
		embedding_profile: "bge-m3-v1",
		embedding_revision: "5617a9f61b028005a4858fdac845db406aefb181",
		dimensions: 1024,
		rerank_profile: "bge-reranker-v2-m3-v1",
		rerank_revision: "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e",
	},
	qdrant: {
		timeout: "10s",
		replication_factor: 1,
		write_consistency_factor: 1,
		write_ordering: "strong",
		read_consistency: "all",
	},
	index: {
		sparse_profile: "bge-m3-lexical-v1",
		batch_size: 16,
		max_result_bytes: 16777216,
		poll_interval: "200ms",
		qualify_interval: "2s",
	},
	retrieval: {
		profile_id: "",
		dense_limit: 40,
		sparse_limit: 40,
		rrf_k: 60,
		fused_limit: 40,
		rerank_limit: 16,
		max_context_chars: 16000,
		max_allowed_sources: 256,
		min_rerank_score: "0",
		max_deadline: "60s",
	},
	store: { schema: "memory_store", max_conn: 4 },
	memory: { max_allowed_facts: 1024, max_projection_epochs: 5, retry_delay: "30s" },
	removals: {
		bucket: "anvilkit-memory-removals",
		region: "us-east-1",
		timeout: "10s",
		window_margin: "5m",
		reconcile_interval: "30s",
	},
	apollo: { mode: "disabled", app_id: "anvilkit-agent-knowledge" },
	reload: { interval: "2s", drain_limit: "30s" },
};

const known = new Set([
	"telemetry.otlp_endpoint",
	"telemetry.sample_ratio",
	"grpc.listen",
	"grpc.capacity",
	"grpc.shutdown_timeout",
	"health.listen",
	"database.url",
	"database.url_file",
	"database.max_conn",
	"tasks.max_input_bytes",
	"tasks.max_lease",
	"tasks.retry_delay",
	"tasks.max_attempts",
	"tasks.sweep_interval",
	"outbox.consumer_group",
	"control.address",
	"control.timeout",
	"objects.endpoint",
	"objects.stage_endpoint",
	"objects.bucket",
	"objects.region",
	"objects.access_key_id",
	"objects.secret_access_key",
	"objects.credentials_file",
	"sources.max_bytes",
	"parser.profile",
	"parser.namespace",
	"parser.node_pool",
	"parser.seccomp_profile",
	"parser.poll_interval",
	"parser.deadline_grace",
	"parser.presign_ttl",
	"parser.kube_timeout",
	"parser.image_registry",
	"parser.kubeconfig",
	"inference.url",
	"inference.timeout",
	"inference.max_batch",
	"inference.embedding_profile",
	"inference.embedding_revision",
	"inference.dimensions",
	"inference.rerank_profile",
	"inference.rerank_revision",
	"qdrant.url",
	"qdrant.api_key",
	"qdrant.timeout",
	"qdrant.replication_factor",
	"qdrant.write_consistency_factor",
	"qdrant.write_ordering",
	"qdrant.read_consistency",
	"index.sparse_profile",
	"index.batch_size",
	"index.max_result_bytes",
	"index.poll_interval",
	"index.qualify_interval",
	"retrieval.profile_id",
	"retrieval.dense_limit",
	"retrieval.sparse_limit",
	"retrieval.rrf_k",
	"retrieval.fused_limit",
	"retrieval.rerank_limit",
	"retrieval.max_context_chars",
	"retrieval.max_allowed_sources",
	"retrieval.min_rerank_score",
	"retrieval.max_deadline",
	"store.url",
	"store.schema",
	"store.max_conn",
	"memory.max_allowed_facts",
	"memory.max_projection_epochs",
	"memory.retry_delay",
	"removals.endpoint",
	"removals.bucket",
	"removals.region",
	"removals.access_key_id",
	"removals.secret_access_key",
	"removals.credentials_file",
	"removals.timeout",
	"removals.window_margin",
	"removals.reconcile_interval",
	"apollo.mode",
	"apollo.snapshot_file",
	"apollo.app_id",
	"reload.interval",
	"reload.drain_limit",
]);

function leaves(raw: Raw, prefix = ""): string[] {
	const out: string[] = [];
	for (const [k, v] of Object.entries(raw)) {
		const path = prefix ? `${prefix}.${k}` : k;
		if (isObject(v)) out.push(...leaves(v, path));
		else out.push(path);
	}
	return out;
}

function merge(into: Raw, from: Raw): void {
	for (const [k, v] of Object.entries(from)) {
		const cur = into[k];
		if (isObject(v) && isObject(cur)) merge(cur, v);
		else into[k] = isObject(v) ? structuredClone(v) : v;
	}
}

/** The reviewed export of one Apollo release (packages/profile-schemas/apollo-snapshot.schema.json). */
export interface ApolloSnapshot {
	schemaVersion: 1;
	appId: string;
	cluster: string;
	namespace: string;
	releaseKey: string;
	fetchedAt: string;
	expiresAt: string;
	configurations: Record<string, string>;
}

const snapshotKeys = new Set([
	"schemaVersion",
	"appId",
	"cluster",
	"namespace",
	"releaseKey",
	"fetchedAt",
	"expiresAt",
	"configurations",
]);

/** Validates a snapshot for this app at `now`; any deviation rejects it. */
export function parseApolloSnapshot(text: string, appId: string, now: Date): ApolloSnapshot {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch (err) {
		throw new ConfigError(`apollo snapshot: ${err instanceof Error ? err.message : String(err)}`);
	}
	if (!isObject(raw)) throw new ConfigError("apollo snapshot: not an object");
	for (const k of Object.keys(raw))
		if (!snapshotKeys.has(k)) throw new ConfigError(`apollo snapshot: unknown field ${k}`);
	for (const k of snapshotKeys) if (!(k in raw)) throw new ConfigError(`apollo snapshot: ${k} is required`);
	if (raw.schemaVersion !== 1) throw new ConfigError("apollo snapshot: schemaVersion is not 1");
	if (raw.appId !== appId)
		throw new ConfigError(`apollo snapshot: appId ${String(raw.appId)} is not this service (${appId})`);
	if (typeof raw.cluster !== "string" || !raw.cluster || typeof raw.namespace !== "string" || !raw.namespace)
		throw new ConfigError("apollo snapshot: cluster and namespace are required");
	if (typeof raw.releaseKey !== "string" || !/^[0-9]{14}-[0-9a-f]{12}$/.test(raw.releaseKey))
		throw new ConfigError("apollo snapshot: releaseKey is not an Apollo release key");
	const fetched = Date.parse(String(raw.fetchedAt));
	const expires = Date.parse(String(raw.expiresAt));
	if (Number.isNaN(fetched) || Number.isNaN(expires))
		throw new ConfigError("apollo snapshot: fetchedAt/expiresAt are RFC3339");
	if (expires <= fetched) throw new ConfigError("apollo snapshot: expiresAt must follow fetchedAt");
	if (expires <= now.getTime())
		throw new ConfigError(
			`apollo snapshot: expired at ${String(raw.expiresAt)}; an expired snapshot never starts a generation`,
		);
	if (!isObject(raw.configurations)) throw new ConfigError("apollo snapshot: configurations are required");
	for (const [k, v] of Object.entries(raw.configurations)) {
		if (!/^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$/.test(k))
			throw new ConfigError(`apollo snapshot: ${k} is not a configuration key`);
		// Keys that name a secret or a placement never come from Apollo
		// (packages/profile-schemas/apollo-snapshot.schema.json).
		if (/(^|\.)(url|url_file|address|token|secret|password|access_key_id|secret_access_key|snapshot_file)$/.test(k))
			throw new ConfigError(`apollo snapshot: ${k} names a secret or a placement and never comes from Apollo`);
		if (typeof v !== "string") throw new ConfigError(`apollo snapshot: ${k} must be a string`);
	}
	return raw as unknown as ApolloSnapshot;
}

function fileDigest(path: string): string {
	try {
		return digestOf(readFileSync(path, "utf8"));
	} catch {
		return "unreadable";
	}
}

function digestOf(text: string): string {
	return `sha256:${createHash("sha256").update(text).digest("hex")}`;
}

function canonical(raw: Raw): string {
	const sortKeys = (v: unknown): unknown =>
		isObject(v)
			? Object.fromEntries(
					Object.keys(v)
						.sort()
						.map((k) => [k, sortKeys(v[k])]),
				)
			: v;
	return JSON.stringify(sortKeys(raw));
}

/** Loads a generation from the file named by the environment (or config.yaml). */
export function load(number = 1, environ: NodeJS.ProcessEnv = process.env): Generation {
	return loadFrom(environ[envConfigFile] || defaultConfigFile, environ, number);
}

/** load with explicit inputs (tests and the reload watcher). */
export function loadFrom(path: string, environ: NodeJS.ProcessEnv, number: number): Generation {
	const raw: Raw = structuredClone(defaults);
	let reviewed: unknown;
	try {
		reviewed = parseYaml(readFileSync(path, "utf8")) ?? {};
	} catch (err) {
		throw new ConfigError(`config file ${path}: ${err instanceof Error ? err.message : String(err)}`);
	}
	if (!isObject(reviewed)) throw new ConfigError(`config file ${path}: not a mapping`);
	for (const key of leaves(reviewed)) {
		if (!known.has(key)) throw new ConfigError(`config file ${path}: unknown key ${key}`);
		if (secretKeys.includes(key) || placementKeys.includes(key))
			throw new ConfigError(
				`config file ${path}: ${key} is a secret or a placement and is accepted only from the environment`,
			);
	}
	merge(raw, reviewed);
	// The environment: allowlisted variables only; unknown ones reject.
	const env: Raw = {};
	const unknownEnv: string[] = [];
	for (const [name, value] of Object.entries(environ)) {
		if (!name.startsWith(envPrefix) || name === envConfigFile || value === undefined) continue;
		const key = envOverrides[name];
		if (!key) {
			unknownEnv.push(name);
			continue;
		}
		set(env, key, value);
	}
	if (unknownEnv.length > 0)
		throw new ConfigError(`environment variables are not allowed overrides: ${unknownEnv.sort().join(", ")}`);
	// The snapshot sits between the file and the rest of the environment.
	let apolloRelease = "";
	let expiresAt: number | undefined;
	const snapshotFile = String(get(env, "apollo.snapshot_file") ?? "");
	if (get(raw, "apollo.mode") === "snapshot") {
		if (!snapshotFile)
			throw new ConfigError(
				"apollo.snapshot_file is required in snapshot mode (ANVILKIT_KNOWLEDGE_APOLLO_SNAPSHOT_FILE)",
			);
		let text: string;
		try {
			text = readFileSync(snapshotFile, "utf8");
		} catch (err) {
			throw new ConfigError(`apollo snapshot: ${err instanceof Error ? err.message : String(err)}`);
		}
		const snap = parseApolloSnapshot(text, String(get(raw, "apollo.app_id")), new Date());
		for (const [key, value] of Object.entries(snap.configurations)) {
			if (!known.has(key)) throw new ConfigError(`apollo snapshot ${snapshotFile}: unknown key ${key}`);
			if (secretKeys.includes(key) || placementKeys.includes(key))
				throw new ConfigError(
					`apollo snapshot ${snapshotFile}: ${key} is a secret or a placement and never comes from Apollo`,
				);
			set(raw, key, value);
		}
		apolloRelease = snap.releaseKey;
		expiresAt = Date.parse(snap.expiresAt);
	}
	merge(raw, env);
	const nonSecret = structuredClone(raw);
	for (const key of secretKeys) {
		const parts = key.split(".");
		const parent = get(nonSecret, parts.slice(0, -1).join(".")) as Raw | undefined;
		if (parent) delete parent[parts[parts.length - 1] as string];
	}
	const digest = digestOf(canonical(nonSecret));
	const cfg = validate(raw);
	return {
		number,
		config: cfg,
		digest,
		secretRevision: digestOf(
			[
				`database.url=${cfg.database.url}`,
				`objects=${cfg.objects.accessKeyId}:${cfg.objects.secretAccessKey}`,
				`kubeconfig=${cfg.parser.kubeconfig ? fileDigest(cfg.parser.kubeconfig) : ""}`,
				`qdrant=${cfg.qdrant.apiKey}`,
				`store=${cfg.store.url}`,
				`removals=${cfg.removals.accessKeyId}:${cfg.removals.secretAccessKey}`,
			].join("\n"),
		),
		profiles: { "local-check-v1": digestOf("local-check-v1") },
		apolloRelease,
		expiresAt,
	};
}

/** The inputs whose change produces a new generation (never a secret value). */
export function inputsOf(g: Generation): string {
	return `${g.digest}/${g.secretRevision}/${g.apolloRelease}/${g.expiresAt ?? ""}`;
}

function str(raw: Raw, key: string): string {
	const v = get(raw, key);
	if (v === undefined || v === null) return "";
	if (typeof v !== "string" && typeof v !== "number") throw new ConfigError(`${key}: must be a string`);
	return String(v);
}

function int(raw: Raw, key: string, min: number, max: number): number {
	const v = get(raw, key);
	const n = typeof v === "string" ? Number(v) : v;
	if (typeof n !== "number" || !Number.isInteger(n) || n < min || n > max)
		throw new ConfigError(`${key}: must be an integer within [${min}, ${max}]`);
	return n;
}

function duration(raw: Raw, key: string, min: number, max: number): number {
	const ms = parseDuration(get(raw, key), key);
	if (ms < min || ms > max) throw new ConfigError(`${key}: must be within [${min}ms, ${max}ms]`);
	return ms;
}

const listenPattern = /^[^:\s]+:\d{1,5}$/;

/** KEY=VALUE lines of a mounted credentials file; only the two credential names are read. */
function credentialsFrom(file: string, key: string, have: { id: string; secret: string }, errors: string[]) {
	if (!file || (have.id && have.secret)) return have;
	let { id, secret } = have;
	try {
		for (const line of readFileSync(file, "utf8").split("\n")) {
			const m = /^([A-Z_]+)=(.*)$/.exec(line.trim());
			if (m?.[1]?.endsWith("ACCESS_KEY_ID")) id ||= m[2] ?? "";
			else if (m?.[1]?.endsWith("SECRET_ACCESS_KEY")) secret ||= m[2] ?? "";
		}
	} catch (err) {
		errors.push(`${key}: ${err instanceof Error ? err.message : String(err)}`);
	}
	return { id, secret };
}

function validate(raw: Raw): Config {
	const errors: string[] = [];
	const attempt = <T>(fn: () => T, fallback: T): T => {
		try {
			return fn();
		} catch (err) {
			errors.push(err instanceof Error ? err.message : String(err));
			return fallback;
		}
	};
	const listen = attempt(() => str(raw, "grpc.listen"), "");
	if (!listenPattern.test(listen)) errors.push("grpc.listen must be host:port");
	const otlpEndpoint = attempt(() => str(raw, "telemetry.otlp_endpoint"), "");
	if (otlpEndpoint && !/^https?:\/\/[^\s/]+(\/[^\s]*)?$/.test(otlpEndpoint))
		errors.push("telemetry.otlp_endpoint must be an http(s) URL of the collector");
	const sampleRatio = Number(get(raw, "telemetry.sample_ratio"));
	if (!(sampleRatio >= 0 && sampleRatio <= 1)) errors.push("telemetry.sample_ratio must be within [0, 1]");
	const health = attempt(() => str(raw, "health.listen"), "");
	if (!listenPattern.test(health)) errors.push("health.listen must be host:port");
	let url = attempt(() => str(raw, "database.url"), "");
	const urlFile = attempt(() => str(raw, "database.url_file"), "");
	if (!url && urlFile) {
		try {
			url = readFileSync(urlFile, "utf8").trim();
		} catch (err) {
			errors.push(`database.url_file: ${err instanceof Error ? err.message : String(err)}`);
		}
	}
	if (!url)
		errors.push("database.url is required (ANVILKIT_KNOWLEDGE_DATABASE_URL or ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE)");
	else if (!/^postgres(ql)?:\/\//.test(url)) errors.push("database.url must be a postgres URL");
	const maxLease = attempt(() => duration(raw, "tasks.max_lease", 1000, 3_600_000), 600_000);
	const sweep = attempt(() => duration(raw, "tasks.sweep_interval", 100, 60_000), 2000);
	if (sweep >= maxLease) errors.push("tasks.sweep_interval must be shorter than tasks.max_lease");
	const mode = attempt(() => str(raw, "apollo.mode"), "disabled");
	if (mode !== "disabled" && mode !== "snapshot") errors.push("apollo.mode must be disabled or snapshot");
	const credentialsFile = attempt(() => str(raw, "objects.credentials_file"), "");
	const objectsCredentials = credentialsFrom(
		credentialsFile,
		"objects.credentials_file",
		{
			id: attempt(() => str(raw, "objects.access_key_id"), ""),
			secret: attempt(() => str(raw, "objects.secret_access_key"), ""),
		},
		errors,
	);
	const accessKeyId = objectsCredentials.id;
	const secretAccessKey = objectsCredentials.secret;
	const objectsEndpoint = attempt(() => str(raw, "objects.endpoint"), "");
	if (objectsEndpoint && !/^https?:\/\//.test(objectsEndpoint)) errors.push("objects.endpoint must be an HTTP(S) URL");
	if (objectsEndpoint && (!accessKeyId || !secretAccessKey))
		errors.push("objects credentials are required with objects.endpoint");
	const parserProfile = attempt(() => str(raw, "parser.profile"), "");
	if (parserProfile && !objectsEndpoint) errors.push("parser.profile requires objects.endpoint");
	const inferenceUrl = attempt(() => str(raw, "inference.url"), "");
	if (inferenceUrl && !/^https?:\/\//.test(inferenceUrl)) errors.push("inference.url must be an HTTP(S) URL");
	const qdrantUrl = attempt(() => str(raw, "qdrant.url"), "");
	if (qdrantUrl && !/^https?:\/\//.test(qdrantUrl)) errors.push("qdrant.url must be an HTTP(S) URL");
	const qdrantKey = attempt(() => str(raw, "qdrant.api_key"), "");
	if (qdrantUrl && !qdrantKey)
		errors.push("qdrant.api_key is required with qdrant.url (ANVILKIT_KNOWLEDGE_QDRANT_API_KEY)");
	const ordering = attempt(() => str(raw, "qdrant.write_ordering"), "strong");
	if (!["weak", "medium", "strong"].includes(ordering))
		errors.push("qdrant.write_ordering must be weak, medium or strong");
	const consistency = attempt(() => str(raw, "qdrant.read_consistency"), "all");
	if (!["all", "majority", "quorum"].includes(consistency))
		errors.push("qdrant.read_consistency must be all, majority or quorum");
	const removalsEndpoint = attempt(() => str(raw, "removals.endpoint"), "");
	if (removalsEndpoint && !/^https?:\/\//.test(removalsEndpoint))
		errors.push("removals.endpoint must be an HTTP(S) URL");
	const removalsCredentialsFile = attempt(() => str(raw, "removals.credentials_file"), "");
	const removalsCredentials = credentialsFrom(
		removalsCredentialsFile,
		"removals.credentials_file",
		{
			id: attempt(() => str(raw, "removals.access_key_id"), ""),
			secret: attempt(() => str(raw, "removals.secret_access_key"), ""),
		},
		errors,
	);
	if (removalsEndpoint && (!removalsCredentials.id || !removalsCredentials.secret))
		errors.push("removals credentials are required with removals.endpoint");
	const storeUrl = attempt(() => str(raw, "store.url"), "");
	if (storeUrl && !/^postgres(ql)?:\/\//.test(storeUrl)) errors.push("store.url must be a postgres URL");
	const minScoreText = attempt(() => str(raw, "retrieval.min_rerank_score"), "0");
	const minScore = Number(minScoreText);
	if (!/^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(minScoreText) || !Number.isFinite(minScore))
		errors.push("retrieval.min_rerank_score must be a decimal");
	const cfg: Config = {
		grpc: {
			listen,
			capacity: attempt(() => int(raw, "grpc.capacity", 1, 4096), 64),
			shutdownTimeoutMs: attempt(() => duration(raw, "grpc.shutdown_timeout", 1, 300_000), 20_000),
		},
		health: { listen: health },
		telemetry: { otlpEndpoint, sampleRatio },
		database: { url, urlFile, maxConn: attempt(() => int(raw, "database.max_conn", 1, 256), 8) },
		tasks: {
			maxInputBytes: attempt(() => int(raw, "tasks.max_input_bytes", 1, 65536), 65536),
			maxLeaseMs: maxLease,
			retryDelayMs: attempt(() => duration(raw, "tasks.retry_delay", 0, 3_600_000), 5000),
			maxAttempts: attempt(() => int(raw, "tasks.max_attempts", 1, 100), 3),
			sweepIntervalMs: sweep,
		},
		outbox: { consumerGroup: attempt(() => str(raw, "outbox.consumer_group"), "") },
		control: {
			address: attempt(() => str(raw, "control.address"), ""),
			timeoutMs: attempt(() => duration(raw, "control.timeout", 1, 60_000), 5000),
		},
		objects: {
			endpoint: objectsEndpoint,
			stageEndpoint: attempt(() => str(raw, "objects.stage_endpoint"), ""),
			bucket: attempt(() => str(raw, "objects.bucket"), ""),
			region: attempt(() => str(raw, "objects.region"), ""),
			accessKeyId,
			secretAccessKey,
			credentialsFile,
		},
		sources: { maxBytes: attempt(() => int(raw, "sources.max_bytes", 1, 268_435_456), 33_554_432) },
		parser: {
			profile: parserProfile,
			namespace: attempt(() => str(raw, "parser.namespace"), ""),
			nodePool: attempt(() => str(raw, "parser.node_pool"), ""),
			seccompProfile: attempt(() => str(raw, "parser.seccomp_profile"), ""),
			pollMs: attempt(() => duration(raw, "parser.poll_interval", 100, 60_000), 1000),
			deadlineGraceMs: attempt(() => duration(raw, "parser.deadline_grace", 0, 600_000), 30_000),
			presignTtlSeconds: Math.floor(
				attempt(() => duration(raw, "parser.presign_ttl", 60_000, 3_600_000), 900_000) / 1000,
			),
			kubeTimeoutMs: attempt(() => duration(raw, "parser.kube_timeout", 100, 120_000), 10_000),
			imageRegistry: attempt(() => str(raw, "parser.image_registry"), ""),
			kubeconfig: attempt(() => str(raw, "parser.kubeconfig"), ""),
		},
		inference: {
			url: inferenceUrl,
			timeoutMs: attempt(() => duration(raw, "inference.timeout", 100, 600_000), 30_000),
			maxBatch: attempt(() => int(raw, "inference.max_batch", 1, 128), 32),
			embeddingProfile: attempt(() => str(raw, "inference.embedding_profile"), ""),
			embeddingRevision: attempt(() => str(raw, "inference.embedding_revision"), ""),
			dimensions: attempt(() => int(raw, "inference.dimensions", 1, 65_536), 1024),
			rerankProfile: attempt(() => str(raw, "inference.rerank_profile"), ""),
			rerankRevision: attempt(() => str(raw, "inference.rerank_revision"), ""),
		},
		qdrant: {
			url: qdrantUrl,
			apiKey: qdrantKey,
			timeoutMs: attempt(() => duration(raw, "qdrant.timeout", 100, 120_000), 10_000),
			replicationFactor: attempt(() => int(raw, "qdrant.replication_factor", 1, 9), 1),
			writeConsistencyFactor: attempt(() => int(raw, "qdrant.write_consistency_factor", 1, 9), 1),
			writeOrdering: ordering as Config["qdrant"]["writeOrdering"],
			readConsistency: consistency as Config["qdrant"]["readConsistency"],
		},
		index: {
			sparseProfile: attempt(() => str(raw, "index.sparse_profile"), ""),
			batchSize: attempt(() => int(raw, "index.batch_size", 1, 128), 16),
			maxResultBytes: attempt(() => int(raw, "index.max_result_bytes", 1024, 268_435_456), 16_777_216),
			pollMs: attempt(() => duration(raw, "index.poll_interval", 0, 60_000), 200),
			qualifyIntervalMs: attempt(() => duration(raw, "index.qualify_interval", 100, 600_000), 2000),
		},
		retrieval: {
			profileId: attempt(() => str(raw, "retrieval.profile_id"), ""),
			denseLimit: attempt(() => int(raw, "retrieval.dense_limit", 1, 1000), 40),
			sparseLimit: attempt(() => int(raw, "retrieval.sparse_limit", 1, 1000), 40),
			rrfK: attempt(() => int(raw, "retrieval.rrf_k", 1, 1000), 60),
			fusedLimit: attempt(() => int(raw, "retrieval.fused_limit", 1, 1000), 40),
			rerankLimit: attempt(() => int(raw, "retrieval.rerank_limit", 1, 128), 16),
			maxContextChars: attempt(() => int(raw, "retrieval.max_context_chars", 1, 1_000_000), 16_000),
			maxAllowedSources: attempt(() => int(raw, "retrieval.max_allowed_sources", 1, 4096), 256),
			minRerankScore: minScore,
			maxDeadlineMs: attempt(() => duration(raw, "retrieval.max_deadline", 100, 600_000), 60_000),
		},
		store: {
			url: storeUrl,
			schema: attempt(() => str(raw, "store.schema"), ""),
			maxConn: attempt(() => int(raw, "store.max_conn", 1, 64), 4),
		},
		memory: {
			maxAllowedFacts: attempt(() => int(raw, "memory.max_allowed_facts", 1, 16_384), 1024),
			maxProjectionEpochs: attempt(() => int(raw, "memory.max_projection_epochs", 1, 100), 5),
			retryDelayMs: attempt(() => duration(raw, "memory.retry_delay", 0, 3_600_000), 30_000),
		},
		removals: {
			endpoint: removalsEndpoint,
			bucket: attempt(() => str(raw, "removals.bucket"), ""),
			region: attempt(() => str(raw, "removals.region"), ""),
			accessKeyId: removalsCredentials.id,
			secretAccessKey: removalsCredentials.secret,
			credentialsFile: removalsCredentialsFile,
			timeoutMs: attempt(() => duration(raw, "removals.timeout", 100, 120_000), 10_000),
			windowMarginMs: attempt(() => duration(raw, "removals.window_margin", 0, 86_400_000), 300_000),
			reconcileIntervalMs: attempt(() => duration(raw, "removals.reconcile_interval", 1000, 3_600_000), 30_000),
		},
		apollo: {
			mode: mode as "disabled" | "snapshot",
			snapshotFile: attempt(() => str(raw, "apollo.snapshot_file"), ""),
			appId: attempt(() => str(raw, "apollo.app_id"), ""),
		},
		reload: {
			intervalMs: attempt(() => duration(raw, "reload.interval", 100, 60_000), 2000),
			drainLimitMs: attempt(() => duration(raw, "reload.drain_limit", 1, 600_000), 30_000),
		},
	};
	if (!cfg.outbox.consumerGroup) errors.push("outbox.consumer_group is required");
	if (!cfg.apollo.appId) errors.push("apollo.app_id is required");
	if (!/^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/.test(cfg.parser.namespace))
		errors.push("parser.namespace is not a namespace");
	if (!cfg.objects.bucket) errors.push("objects.bucket is required");
	if (!cfg.removals.bucket) errors.push("removals.bucket is required");
	if (
		cfg.removals.endpoint &&
		cfg.removals.endpoint === cfg.objects.endpoint &&
		cfg.removals.bucket === cfg.objects.bucket
	)
		errors.push(
			"removals.bucket must not be the objects bucket (the removal inventory is independent of the data it outlives)",
		);
	if (!/^[a-z][a-z0-9_]{0,62}$/.test(cfg.store.schema)) errors.push("store.schema must be a plain identifier");
	if (cfg.qdrant.writeConsistencyFactor > cfg.qdrant.replicationFactor)
		errors.push("qdrant.write_consistency_factor must not exceed qdrant.replication_factor");
	if (cfg.retrieval.fusedLimit > cfg.retrieval.denseLimit + cfg.retrieval.sparseLimit)
		errors.push("retrieval.fused_limit must not exceed the two branch limits together");
	if (cfg.retrieval.rerankLimit > cfg.retrieval.fusedLimit)
		errors.push("retrieval.rerank_limit must not exceed retrieval.fused_limit");
	if (cfg.index.batchSize > cfg.inference.maxBatch) errors.push("index.batch_size must not exceed inference.max_batch");

	if (errors.length > 0) throw new ConfigError(`config: ${errors.join("; ")}`);
	return cfg;
}
