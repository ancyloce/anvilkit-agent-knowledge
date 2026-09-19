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
	ANVILKIT_KNOWLEDGE_DATABASE_URL: "database.url",
	ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE: "database.url_file",
	ANVILKIT_KNOWLEDGE_CONTROL_ADDRESS: "control.address",
	ANVILKIT_KNOWLEDGE_APOLLO_SNAPSHOT_FILE: "apollo.snapshot_file",
};

const secretKeys = ["database.url"];
const placementKeys = ["database.url_file", "control.address", "apollo.snapshot_file"];

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
	database: { max_conn: 8 },
	tasks: { max_input_bytes: 65536, max_lease: "10m", retry_delay: "5s", max_attempts: 3, sweep_interval: "2s" },
	outbox: { consumer_group: "anvilkit-agent-knowledge-forwarder" },
	control: { timeout: "5s" },
	apollo: { mode: "disabled", app_id: "anvilkit-agent-knowledge" },
	reload: { interval: "2s", drain_limit: "30s" },
};

const known = new Set([
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
		secretRevision: digestOf(`database.url=${cfg.database.url}`),
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
	const cfg: Config = {
		grpc: {
			listen,
			capacity: attempt(() => int(raw, "grpc.capacity", 1, 4096), 64),
			shutdownTimeoutMs: attempt(() => duration(raw, "grpc.shutdown_timeout", 1, 300_000), 20_000),
		},
		health: { listen: health },
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
	if (errors.length > 0) throw new ConfigError(`config: ${errors.join("; ")}`);
	return cfg;
}
