// Knowledge's data access (contracts.md §4 "Node uses one pg PoolClient"):
// one controlled client per owner transaction, the reviewed statements over
// it and, on the same client and transaction, the outbox insert in the
// physical schema of the pinned watermill-sql v4.1.5 PostgreSQL adapter
// (migration 00002: outbox/outbox_offsets). The runtime role has DML only;
// nothing here creates or alters schema.
import pg from "pg";
import type { Envelope } from "../domain/event.js";
import type { Attempt, Task, TaskState } from "../domain/task.js";

const { Pool } = pg;
export type PoolClient = pg.PoolClient;

/** The forwarder topic and the adapter's envelope, as the Go forwarder unwraps it. */
export const forwarderTopic = "outbox";

export const metadataKeys = {
	eventType: "anvilkit_event_type",
	tenantId: "anvilkit_tenant_id",
	schemaVersion: "anvilkit_schema_version",
} as const;

export class Store {
	private current: pg.Pool;

	constructor(pool: pg.Pool) {
		this.current = pool;
	}

	/** The active generation's pool. */
	get pool(): pg.Pool {
		return this.current;
	}

	/** Publishes the pool of a new generation; the previous one is returned for draining. */
	swap(next: pg.Pool): pg.Pool {
		const previous = this.current;
		this.current = next;
		return previous;
	}

	/** Runs fn in one read-committed transaction on one client. */
	async inTx<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
		const client = await this.current.connect();
		try {
			await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
			const out = await fn(client);
			await client.query("COMMIT");
			return out;
		} catch (err) {
			await client.query("ROLLBACK").catch(() => undefined);
			throw err;
		} finally {
			client.release();
		}
	}
}

export function newPool(url: string, maxConn: number): pg.Pool {
	return new Pool({ connectionString: url, max: maxConn, allowExitOnIdle: false });
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

interface RequestRow {
	task_id: string;
	generation: string;
	tenant_id: string;
	task_kind: string;
	input_digest: string;
	input: string;
	state: string;
	worker_id: string | null;
	lease_until: Date | null;
	attempt_count: number;
	max_attempts: number;
	result_profile: string;
	effects: string;
	dispatch_id: string | null;
	authorization_ref: string;
	retry_at: Date | null;
	result_ref: string | null;
	result_digest: string | null;
	failure_code: string | null;
	revision: string;
	correlation_id: string;
}

const requestColumns =
	"task_id, generation::text AS generation, tenant_id, task_kind, input_digest, input::text AS input, state, worker_id, lease_until, attempt_count, max_attempts, result_profile, effects, dispatch_id, authorization_ref, retry_at, result_ref, result_digest, failure_code, revision::text AS revision, correlation_id";

function taskFromRow(r: RequestRow): Task {
	return {
		taskId: r.task_id,
		generation: Number(r.generation),
		tenantId: r.tenant_id,
		kind: r.task_kind as Task["kind"],
		inputDigest: r.input_digest,
		input: r.input,
		state: r.state as TaskState,
		workerId: r.worker_id ?? "",
		leaseUntil: r.lease_until,
		attemptCount: r.attempt_count,
		maxAttempts: r.max_attempts,
		resultProfile: r.result_profile,
		effects: r.effects as Task["effects"],
		dispatchId: r.dispatch_id ?? "",
		authorizationRef: r.authorization_ref,
		retryAt: r.retry_at,
		resultRef: r.result_ref ?? "",
		resultDigest: r.result_digest ?? "",
		failureCode: r.failure_code ?? "",
		revision: Number(r.revision),
		correlationId: r.correlation_id,
	};
}

interface AttemptRow {
	ordinal: number;
	worker_id: string;
	leased_at: Date;
	lease_until: Date;
	outcome: string | null;
}

/** jsonb canonical text of the input: the bytes a claim returns are the bytes the digest binds. */
export async function canonicalJson(c: PoolClient, input: string): Promise<string> {
	const r = await c.query<{ canonical: string }>("SELECT $1::jsonb::text AS canonical", [input]);
	return r.rows[0]?.canonical ?? "";
}

export async function getRequestForUpdate(
	c: PoolClient,
	taskId: string,
	generation: number,
): Promise<Task | undefined> {
	const r = await c.query<RequestRow>(
		`SELECT ${requestColumns} FROM background_requests WHERE task_id = $1 AND generation = $2 FOR UPDATE`,
		[taskId, generation],
	);
	return r.rows[0] ? taskFromRow(r.rows[0]) : undefined;
}

export async function getRequest(
	c: pg.Pool | PoolClient,
	taskId: string,
	generation: number,
): Promise<Task | undefined> {
	const r = await c.query<RequestRow>(
		`SELECT ${requestColumns} FROM background_requests WHERE task_id = $1 AND generation = $2`,
		[taskId, generation],
	);
	return r.rows[0] ? taskFromRow(r.rows[0]) : undefined;
}

export async function getLatestRequest(
	c: pg.Pool | PoolClient,
	taskId: string,
	forUpdate = false,
): Promise<Task | undefined> {
	const r = await c.query<RequestRow>(
		`SELECT ${requestColumns} FROM background_requests WHERE task_id = $1 ORDER BY generation DESC LIMIT 1${forUpdate ? " FOR UPDATE" : ""}`,
		[taskId],
	);
	return r.rows[0] ? taskFromRow(r.rows[0]) : undefined;
}

export async function insertRequest(c: PoolClient, t: Task): Promise<void> {
	await c.query(
		`INSERT INTO background_requests (task_id, generation, tenant_id, task_kind, input_digest, input, state, attempt_count, max_attempts, result_profile, effects, dispatch_id, authorization_ref, revision, correlation_id)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, 0, $8, $9, $10, $11, $12, 1, $13)`,
		[
			t.taskId,
			t.generation,
			t.tenantId,
			t.kind,
			t.inputDigest,
			t.input,
			t.state,
			t.maxAttempts,
			t.resultProfile,
			t.effects,
			t.dispatchId || null,
			t.authorizationRef,
			t.correlationId,
		],
	);
}

/** The CAS update: binds the revision the transaction read. */
export async function updateRequest(c: PoolClient, t: Task, readRevision: number, now: Date): Promise<number> {
	const r = await c.query(
		`UPDATE background_requests SET state = $3, worker_id = $4, lease_until = $5, attempt_count = $6, retry_at = $7,
		   result_ref = $8, result_digest = $9, failure_code = $10, revision = $11, completed_at = $12, updated_at = now()
		 WHERE task_id = $1 AND generation = $2 AND revision = $13`,
		[
			t.taskId,
			t.generation,
			t.state,
			t.workerId || null,
			t.state === "leased" ? t.leaseUntil : null,
			t.attemptCount,
			t.retryAt,
			t.resultRef || null,
			t.resultDigest || null,
			t.failureCode || null,
			t.revision,
			["accepted", "dead", "stale", "canceled"].includes(t.state) ? now : null,
			readRevision,
		],
	);
	return r.rowCount ?? 0;
}

export async function listAttempts(c: PoolClient, taskId: string, generation: number): Promise<Attempt[]> {
	const r = await c.query<AttemptRow>(
		"SELECT ordinal, worker_id, leased_at, lease_until, outcome FROM task_attempts WHERE task_id = $1 AND generation = $2 ORDER BY ordinal",
		[taskId, generation],
	);
	return r.rows.map((a) => ({
		ordinal: a.ordinal,
		workerId: a.worker_id,
		leasedAt: a.leased_at,
		leaseUntil: a.lease_until,
		outcome: a.outcome ?? "",
	}));
}

export async function insertAttempt(c: PoolClient, taskId: string, generation: number, a: Attempt): Promise<void> {
	await c.query(
		"INSERT INTO task_attempts (task_id, generation, ordinal, worker_id, leased_at, lease_until) VALUES ($1, $2, $3, $4, $5, $6)",
		[taskId, generation, a.ordinal, a.workerId, a.leasedAt, a.leaseUntil],
	);
}

export async function setAttemptOutcome(
	c: PoolClient,
	taskId: string,
	generation: number,
	ordinal: number,
	outcome: string,
	now: Date,
): Promise<void> {
	await c.query(
		"UPDATE task_attempts SET outcome = $4, submitted_at = $5 WHERE task_id = $1 AND generation = $2 AND ordinal = $3",
		[taskId, generation, ordinal, outcome, now],
	);
}

export async function listExpiredLeases(
	pool: pg.Pool,
	now: Date,
	limit: number,
): Promise<{ taskId: string; generation: number }[]> {
	const r = await pool.query<{ task_id: string; generation: string }>(
		"SELECT task_id, generation::text AS generation FROM background_requests WHERE state = 'leased' AND lease_until < $1 ORDER BY lease_until LIMIT $2",
		[now, limit],
	);
	return r.rows.map((x) => ({ taskId: x.task_id, generation: Number(x.generation) }));
}

/**
 * The owner's current authorization for a source:<source_id> reference from
 * the sources table of this transaction: a registered, undeleted source of
 * the request's tenant is current; a deleted, missing or foreign one is not
 * (DD-07; the P15 source lifecycle writes the rows).
 */
export async function sourceAuthorization(
	c: PoolClient,
	ref: string,
	tenantId: string,
): Promise<"current" | "revoked"> {
	// A memory-project request is authorized by its fact existing in the
	// tenant: a revoked or deleted fact still needs its tombstone applied,
	// and the projection itself checks the fact's current revision (P17).
	if (ref.startsWith("memory:")) {
		const m = await c.query<{ tenant_id: string }>("SELECT tenant_id FROM memory_facts WHERE fact_id = $1", [
			ref.slice("memory:".length),
		]);
		return m.rows[0]?.tenant_id === tenantId ? "current" : "revoked";
	}
	if (!ref.startsWith("source:")) return "revoked";
	const r = await c.query<{ deleted: boolean; tenant_id: string }>(
		"SELECT deleted, tenant_id FROM sources WHERE source_id = $1",
		[ref.slice("source:".length)],
	);
	const row = r.rows[0];
	if (!row || row.deleted || row.tenant_id !== tenantId) return "revoked";
	return "current";
}

/**
 * The outbox insert of the same transaction (DD-09 §2): the forwarder
 * envelope of the watermill forwarder component (destination_topic, the
 * event's uuid, the base64 payload, its metadata) in the adapter's row —
 * a fresh row uuid, the wrapper's empty metadata and pg_current_xact_id()
 * as the adapter's insert expression, so the Go subscriber reads it
 * exactly as a Go-published row.
 */
export async function publishOutbox(c: PoolClient, env: Envelope): Promise<void> {
	const wrapped = {
		destination_topic: env.subject,
		uuid: env.eventId,
		payload: Buffer.from(JSON.stringify(env)).toString("base64"),
		metadata: {
			[metadataKeys.eventType]: env.eventType,
			[metadataKeys.tenantId]: env.tenantId,
			[metadataKeys.schemaVersion]: "1",
		},
	};
	await c.query(
		"INSERT INTO outbox (uuid, payload, metadata, transaction_id) VALUES ($1, $2, $3, pg_current_xact_id())",
		[crypto.randomUUID(), JSON.stringify(wrapped), "{}"],
	);
}

export interface Observation {
	byState: Record<string, number>;
	overdueRetries: number;
	oldestUnforwardedSeconds: number;
}

/** The gauges the sweeper refreshes. */
export async function observe(pool: pg.Pool, consumerGroup: string, now: Date): Promise<Observation> {
	const counts = await pool.query<{ state: string; n: string }>(
		"SELECT state, count(*)::text AS n FROM background_requests GROUP BY state",
	);
	const overdue = await pool.query<{ n: string }>(
		"SELECT count(*)::text AS n FROM background_requests WHERE state = 'retry_scheduled' AND retry_at < $1",
		[now],
	);
	const oldest = await pool.query<{ seconds: number }>(
		`SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::float8 AS seconds FROM outbox
		 WHERE (transaction_id, "offset") > (SELECT COALESCE(max(last_processed_transaction_id), '0'::xid8), COALESCE(max(offset_acked), 0) FROM outbox_offsets WHERE consumer_group = $1)`,
		[consumerGroup],
	);
	return {
		byState: Object.fromEntries(counts.rows.map((r) => [r.state, Number(r.n)])),
		overdueRetries: Number(overdue.rows[0]?.n ?? 0),
		oldestUnforwardedSeconds: Number(oldest.rows[0]?.seconds ?? 0),
	};
}
