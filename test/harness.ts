// A disposable PostgreSQL 17 with anvilkit_knowledge installed from the
// migration source (the parent repository's migration Job,
// jobs/migration/internal/migrate/sql/knowledge, found beside this
// repository or named by ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR): the "+goose Up"
// sections applied in order by the migrator role, exactly the DDL the Job
// applies. Tests skip when Docker or the directory is unavailable.
import { existsSync, readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { Registry } from "prom-client";
import { newPool, Store } from "../src/adapters/postgres.js";
import { type Clock, type DispatchQuery, Tasks } from "../src/application/tasks.js";
import type { Bounds, DispatchOutcome } from "../src/domain/task.js";
import { silentLogger } from "../src/log.js";
import { Metrics } from "../src/metrics.js";

export function migrationsDir(): string | undefined {
	const env = process.env.ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR;
	if (env) return env;
	let d = process.cwd();
	for (;;) {
		const candidate = path.join(d, "jobs", "migration", "internal", "migrate", "sql", "knowledge");
		if (existsSync(path.join(candidate, "00001_init.sql"))) return candidate;
		const parent = path.dirname(d);
		if (parent === d) return undefined;
		d = parent;
	}
}

export interface Instance {
	container: StartedPostgreSqlContainer;
	adminUrl: string;
	appUrl: string;
	relayUrl: string;
	forwarderUrl: string;
	admin(sql: string, params?: unknown[]): Promise<pg.QueryResult>;
	/** A statement as the cluster superuser (role changes, the rotation fixture). */
	root(sql: string): Promise<void>;
	stop(): Promise<void>;
}

export async function startInstance(): Promise<Instance> {
	const dir = migrationsDir();
	if (!dir)
		throw new Error("UNEXECUTED: anvilkit_knowledge migrations not found (set ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR)");
	const container = await new PostgreSqlContainer("postgres:17-alpine")
		.withDatabase("postgres")
		.withUsername("postgres")
		.withPassword("postgres")
		.start();
	const adminUrl = container.getConnectionUri();
	const root = new pg.Client({ connectionString: adminUrl });
	await root.connect();
	for (const s of [
		"CREATE ROLE anvilkit_knowledge_app LOGIN PASSWORD 'app'",
		"CREATE ROLE anvilkit_knowledge_migrator LOGIN PASSWORD 'migrator'",
		"CREATE ROLE anvilkit_knowledge_relay LOGIN PASSWORD 'relay'",
		"CREATE ROLE anvilkit_knowledge_forwarder LOGIN PASSWORD 'forwarder'",
		"CREATE DATABASE anvilkit_knowledge OWNER anvilkit_knowledge_migrator",
	])
		await root.query(s);
	await root.end();
	const url = (role: string, pw: string) =>
		`postgres://${role}:${pw}@${container.getHost()}:${container.getPort()}/anvilkit_knowledge`;
	const migrator = new pg.Client({ connectionString: url("anvilkit_knowledge_migrator", "migrator") });
	await migrator.connect();
	await migrator.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
	for (const file of readdirSync(dir)
		.filter((f) => /^\d+_.*\.sql$/.test(f))
		.sort()) {
		const text = readFileSync(path.join(dir, file), "utf8");
		const up = text.split("-- +goose Down")[0]?.replace("-- +goose Up", "") ?? "";
		await migrator.query(up);
	}
	await migrator.end();
	const adminClient = new pg.Client({ connectionString: url("anvilkit_knowledge_migrator", "migrator") });
	await adminClient.connect();
	return {
		container,
		adminUrl,
		appUrl: url("anvilkit_knowledge_app", "app"),
		relayUrl: url("anvilkit_knowledge_relay", "relay"),
		forwarderUrl: url("anvilkit_knowledge_forwarder", "forwarder"),
		admin: (sql, params) => adminClient.query(sql, params),
		root: async (sql) => {
			const c = new pg.Client({ connectionString: adminUrl });
			await c.connect();
			try {
				await c.query(sql);
			} finally {
				await c.end();
			}
		},
		stop: async () => {
			await adminClient.end();
			await container.stop();
		},
	};
}

/** Inserts the source row a source:<id> authorization needs (the P15 lifecycle writes them for real). */
export async function sourceFixture(inst: Instance, tenant: string, sourceId: string): Promise<void> {
	await inst.admin(
		`INSERT INTO sources (source_id, tenant_id, project_id, kind, locator, command_id, request_digest)
		 VALUES ($1, $2, 'proj_a', 'document', 'file://fixture', $3, 'sha256:0000000000000000000000000000000000000000000000000000000000000000') ON CONFLICT DO NOTHING`,
		[sourceId, tenant, `cmd_${sourceId}`],
	);
}

export class FakeClock implements Clock {
	constructor(private at: Date) {}
	now(): Date {
		return this.at;
	}
	advance(ms: number): void {
		this.at = new Date(this.at.getTime() + ms);
	}
}

export class FakeDispatch implements DispatchQuery {
	answer: DispatchOutcome = "unknown";
	calls = 0;
	outcome(): Promise<DispatchOutcome> {
		this.calls++;
		return Promise.resolve(this.answer);
	}
}

export const bounds: Bounds = { maxInputBytes: 65536, maxLeaseMs: 3_600_000, retryDelayMs: 5000, maxAttempts: 2 };

export function newTasks(
	inst: Instance,
	clock: Clock,
	dispatch: DispatchQuery,
): { tasks: Tasks; store: Store; metrics: Metrics; registry: Registry } {
	const store = new Store(newPool(inst.appUrl, 4));
	const registry = new Registry();
	const metrics = new Metrics(registry);
	return { tasks: new Tasks(store, dispatch, bounds, clock, silentLogger, metrics), store, metrics, registry };
}

export function localCheckInput(payload: string, extra: Record<string, unknown> = {}): string {
	return JSON.stringify({
		schemaVersion: 1,
		computation: "local-check-v1",
		bytes: Buffer.from(payload).toString("base64"),
		...extra,
	});
}
