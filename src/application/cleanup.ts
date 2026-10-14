// P2.7: the erasure of deleted sources and the retention of delivery rows,
// both under the cleanup identity (anvilkit_knowledge_cleanup), the only
// role that deletes rows; the app role never does.
//
// Erasure: after DeleteSource blocked readability and the indexer removed
// the source's points from every generation (points_purged_at), the purge
// deletes its upload object, every content-addressed copy that no live
// revision of the tenant shares, the parse results of its launches, and its
// chunk rows, then marks it purged (purged_at). Backups and the DR store
// keep what they held until their own expiry; third-party retention (a
// provider that saw text in a prompt) is outside this erasure.
//
// Retention: acknowledged outbox rows, inbox entries past the redelivery
// horizon, the attempts of terminal requests and the launches of terminal
// parses, in bounded batches.
import type pg from "pg";
import type { ObjectStore } from "../adapters/objects.js";
import type * as db from "../adapters/postgres.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import type { Clock } from "./tasks.js";

export interface CleanupBounds {
	batch: number;
	outboxRetentionMs: number;
	inboxRetentionMs: number;
	attemptsRetentionMs: number;
	launchesRetentionMs: number;
	consumerGroup: string;
}

export interface PurgeStatus {
	sourceId: string;
	/** live: not deleted; pending: deleted, erasure not finished; purged: erased. */
	status: "live" | "pending" | "purged";
	deletedAt: Date | null;
	pointsPurgedAt: Date | null;
	purgedAt: Date | null;
}

/** The reconciler's lock key (one replica runs index, removal, memory, purge and retention passes). */
export const reconcilerLock = 0x6b6e6f776c656467n; // "knowledg"

/**
 * Runs fn while this process holds the session advisory lock on a client of
 * its own; answers false (fn not run) when another replica holds it.
 */
export async function withLock(pool: pg.Pool, key: bigint, fn: () => Promise<void>): Promise<boolean> {
	const client = await pool.connect();
	try {
		const r = await client.query<{ ok: boolean }>("SELECT pg_try_advisory_lock($1::bigint) AS ok", [key.toString()]);
		if (!r.rows[0]?.ok) return false;
		try {
			await fn();
		} finally {
			await client.query("SELECT pg_advisory_unlock($1::bigint)", [key.toString()]).catch(() => undefined);
		}
		return true;
	} finally {
		client.release();
	}
}

export class Cleanup {
	constructor(
		private readonly store: db.Store,
		private readonly cleanup: () => pg.Pool | undefined,
		private readonly objects: () => ObjectStore | undefined,
		private readonly bounds: () => CleanupBounds,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	/** Erases up to `limit` deleted sources whose points are gone; answers how many it finished. */
	async purgeDeleted(limit = 20): Promise<number> {
		const pool = this.cleanup();
		const objects = this.objects();
		if (!pool || !objects) return 0;
		const due = await this.store.pool.query<{ source_id: string; tenant_id: string; locator: string }>(
			`SELECT source_id, tenant_id, locator FROM sources
			 WHERE deleted AND points_purged_at IS NOT NULL AND purged_at IS NULL ORDER BY deleted_at LIMIT $1`,
			[limit],
		);
		let done = 0;
		for (const s of due.rows) {
			try {
				await this.purge(pool, objects, s.source_id, s.tenant_id, s.locator);
				done++;
				this.metrics.indexBatches.inc({ outcome: "source_purged" });
			} catch (err) {
				// Asked again on the next pass; a partial erasure is completed then.
				this.log.warn("source not purged yet", { sourceId: s.source_id, error: String(err).slice(0, 200) });
			}
		}
		return done;
	}

	private async purge(
		pool: pg.Pool,
		objects: ObjectStore,
		sourceId: string,
		tenantId: string,
		locator: string,
	): Promise<void> {
		const keys = new Set<string>();
		if (locator.startsWith("upload:")) keys.add(`uploads/${tenantId}/${locator.slice("upload:".length)}`);
		// A content-addressed copy another live source of the tenant shares stays.
		const copies = await this.store.pool.query<{ object_key: string }>(
			`SELECT DISTINCT r.object_key FROM source_revisions r WHERE r.source_id = $1
			   AND NOT EXISTS (SELECT 1 FROM source_revisions o JOIN sources x ON x.source_id = o.source_id
			                   WHERE o.object_key = r.object_key AND o.source_id <> $1 AND NOT x.deleted)`,
			[sourceId],
		);
		for (const r of copies.rows) keys.add(r.object_key);
		const launches = await this.store.pool.query<{ result_key: string | null; launch_key: string }>(
			`SELECT p.result_key, p.launch_key FROM parse_launches p JOIN ingest_requests i ON i.task_id = p.task_id
			 WHERE i.source_id = $1`,
			[sourceId],
		);
		for (const l of launches.rows) keys.add(l.result_key || `parse/${l.launch_key}/result.json`);
		for (const key of keys) await objects.delete(key);
		await pool.query("DELETE FROM chunks WHERE source_id = $1", [sourceId]);
		const marked = await this.store.pool.query(
			"UPDATE sources SET purged_at = $2, updated_at = now() WHERE source_id = $1 AND deleted AND points_purged_at IS NOT NULL AND purged_at IS NULL",
			[sourceId, this.clock.now()],
		);
		if ((marked.rowCount ?? 0) === 1) this.log.info("source purged", { sourceId, objects: keys.size });
	}

	/** The erasure state of one source (knowledgectl purge-status). */
	async status(sourceId: string): Promise<PurgeStatus | undefined> {
		const r = await this.store.pool.query<{
			deleted: boolean;
			deleted_at: Date | null;
			points_purged_at: Date | null;
			purged_at: Date | null;
		}>("SELECT deleted, deleted_at, points_purged_at, purged_at FROM sources WHERE source_id = $1", [sourceId]);
		const row = r.rows[0];
		if (!row) return undefined;
		return {
			sourceId,
			status: !row.deleted ? "live" : row.purged_at ? "purged" : "pending",
			deletedAt: row.deleted_at,
			pointsPurgedAt: row.points_purged_at,
			purgedAt: row.purged_at,
		};
	}

	/** Prunes delivery rows past their retention, a bounded batch per table; answers the counts. */
	async retain(): Promise<Record<string, number>> {
		const pool = this.cleanup();
		if (!pool) return {};
		const b = this.bounds();
		const now = this.clock.now().getTime();
		const before = (ms: number) => new Date(now - ms);
		const out: Record<string, number> = {};
		// Outbox rows the forwarder acknowledged (at or before its position).
		out.outbox =
			(
				await pool.query(
					`DELETE FROM outbox WHERE ctid IN (
					   SELECT o.ctid FROM outbox o JOIN outbox_offsets f ON f.consumer_group = $1
					   WHERE (o.transaction_id < f.last_processed_transaction_id
					          OR (o.transaction_id = f.last_processed_transaction_id AND o."offset" <= COALESCE(f.offset_acked, 0)))
					     -- created_at is watermill's timestamp without time zone, written in
					     -- the session's zone: the bound is compared in that zone.
					     AND o.created_at < ($2::timestamptz AT TIME ZONE current_setting('TimeZone')) LIMIT $3)`,
					[b.consumerGroup, before(b.outboxRetentionMs), b.batch],
				)
			).rowCount ?? 0;
		out.inbox =
			(
				await pool.query("DELETE FROM inbox WHERE ctid IN (SELECT ctid FROM inbox WHERE accepted_at < $1 LIMIT $2)", [
					before(b.inboxRetentionMs),
					b.batch,
				])
			).rowCount ?? 0;
		out.attempts =
			(
				await pool.query(
					`DELETE FROM task_attempts WHERE ctid IN (
					   SELECT a.ctid FROM task_attempts a JOIN background_requests r ON r.task_id = a.task_id AND r.generation = a.generation
					   WHERE r.state IN ('accepted', 'dead', 'stale', 'canceled') AND r.updated_at < $1 LIMIT $2)`,
					[before(b.attemptsRetentionMs), b.batch],
				)
			).rowCount ?? 0;
		out.launches =
			(
				await pool.query(
					`DELETE FROM parse_launches WHERE ctid IN (
					   SELECT p.ctid FROM parse_launches p JOIN background_requests r ON r.task_id = p.task_id AND r.generation = p.generation
					   WHERE p.state IN ('completed', 'failed') AND r.state IN ('accepted', 'dead', 'stale', 'canceled')
					     AND p.updated_at < $1 LIMIT $2)`,
					[before(b.launchesRetentionMs), b.batch],
				)
			).rowCount ?? 0;
		const total = Object.values(out).reduce((a, n) => a + n, 0);
		if (total > 0) this.log.info("delivery rows pruned", out);
		return out;
	}
}
