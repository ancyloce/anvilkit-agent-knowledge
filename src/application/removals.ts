// Removal preservation across a database restore (P23; platform.md §6,
// execution.md §4, SEC-06, SEC-11). A deletion or revocation commits its
// removal row with the decision; its immutable record is then created in
// the removal inventory (the independent DR store) before the caller is
// answered, and a record whose write was not confirmed stays pending until
// a retry of the command or the next pass confirms it.
//
// After a point-in-time restore the database no longer holds the removals
// committed after its restore point, but the inventory does. Each pass
// lists the inventory from the newest removal the database still knows,
// less the clock margin (a decision may commit after a newer one), and
// re-applies every record without a row under its original command
// identity. Until one pass has listed that window completely, memory is
// closed: no fact is read, decided, recalled or projected, so nothing the
// restore revived can be disclosed or written to a projection. A failed or
// truncated listing never counts as an empty one. A database watermark
// that moves back (a restore under a running process) closes memory again.
import * as mdb from "../adapters/memorydb.js";
import type * as db from "../adapters/postgres.js";
import { RemovalConflict, type RemovalInventory, RemovalListIncomplete } from "../adapters/removals.js";
import { MemoryError } from "../domain/memory.js";
import {
	maxRemovalBytes,
	parseRemoval,
	type RemovalRecord,
	removalBodyOf,
	removalKeyFloor,
	removalKeyOf,
	removalPrefixOf,
} from "../domain/removal.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import type { Clock } from "./tasks.js";

export interface RemovalBounds {
	windowMarginMs: number;
	reconcileIntervalMs: number;
	/** How long a pending row is left to its command's own write before a pass writes it (default 5 s). */
	pendingGraceMs?: number;
}

/** What re-applying one record did (the Memory use case owns the decision). */
export type RestoreOutcome = "restored" | "held" | "absent" | "known";

export interface RestoreTarget {
	applyRestored(r: RemovalRecord, key: string): Promise<RestoreOutcome>;
}

export interface PassResult {
	listed: number;
	restored: number;
	recorded: number;
}

const pageSize = 500;
/** A pending row younger than this is still being written by its command. */
const defaultPendingGraceMs = 5_000;

export class Removals {
	private reconciled = false;
	private highWater: number | undefined;
	private lastPass: number | undefined;
	private target: RestoreTarget | undefined;

	constructor(
		private readonly store: db.Store,
		private readonly inventory: () => RemovalInventory | undefined,
		private readonly bounds: () => RemovalBounds,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	setTarget(t: RestoreTarget): void {
		this.target = t;
	}

	/** Whether memory may serve: no inventory is placed, or a pass completed since the last restore. */
	ready(): boolean {
		return this.inventory() === undefined || this.reconciled;
	}

	check(): void {
		if (!this.ready())
			throw new MemoryError("UNAVAILABLE", "memory is closed until the removal inventory is reconciled");
	}

	// ---------------------------------------------------------------------
	// Recording
	// ---------------------------------------------------------------------

	/**
	 * Creates the record of a committed removal and marks its row recorded.
	 * Unplaced, the row stays pending (recorded once an inventory is placed).
	 */
	async record(r: RemovalRecord, key: string): Promise<void> {
		const inv = this.inventory();
		if (!inv) return;
		try {
			await inv.create(key, removalBodyOf(r));
		} catch (err) {
			this.metrics.memoryRemovals.inc({ outcome: err instanceof RemovalConflict ? "conflict" : "uncertain" });
			if (err instanceof RemovalConflict)
				this.log.error("removal record conflict: another record exists under its key", { factId: r.factId });
			throw err;
		}
		await mdb.markRemovalRecorded(this.store.pool, r.tenantId, r.commandId);
		this.metrics.memoryRemovals.inc({ outcome: "recorded" });
	}

	/** A replayed removal command completes its record if it is still pending. */
	async ensureRecorded(tenantId: string, commandId: string): Promise<void> {
		const row = await mdb.removalByCommand(this.store.pool, tenantId, commandId);
		if (row?.state === "pending") await this.recordRow(row);
	}

	private async recordRow(row: mdb.RemovalRow): Promise<void> {
		let key = row.inventoryKey;
		if (!key) {
			key = removalKeyOf(await mdb.removalScope(this.store.pool), row.record);
			await mdb.setRemovalKey(this.store.pool, row.record.tenantId, row.record.commandId, key);
		}
		await this.record(row.record, key);
	}

	// ---------------------------------------------------------------------
	// Reconciliation
	// ---------------------------------------------------------------------

	/** One pass when due: always while memory is closed, otherwise every reconcile interval. */
	async tick(): Promise<PassResult | undefined> {
		const inv = this.inventory();
		this.metrics.memoryRemovalsReconciled.set(this.ready() ? 1 : 0);
		if (!inv) return undefined;
		const now = this.clock.now().getTime();
		if (this.reconciled && this.lastPass !== undefined && now - this.lastPass < this.bounds().reconcileIntervalMs)
			return undefined;
		return this.pass(inv);
	}

	/** Lists the window, re-applies what the database lacks, then completes pending records. */
	async pass(inv: RemovalInventory): Promise<PassResult> {
		const target = this.target;
		if (!target) throw new Error("no restore target wired");
		const scope = await mdb.removalScope(this.store.pool);
		const watermark = await mdb.removalWatermark(this.store.pool);
		if (this.highWater !== undefined && (watermark === undefined || watermark.getTime() < this.highWater)) {
			if (this.reconciled)
				this.log.warn("the database's newest removal moved back (a restore?): memory is closed until reconciled", {});
			this.reconciled = false;
			this.metrics.memoryRemovalsReconciled.set(0);
		}
		const prefix = removalPrefixOf(scope);
		const from =
			watermark === undefined
				? ""
				: removalKeyFloor(scope, new Date(watermark.getTime() - this.bounds().windowMarginMs));
		const out: PassResult = { listed: 0, restored: 0, recorded: 0 };
		let next: string | undefined;
		do {
			const page = await inv.list(prefix, from, next, pageSize);
			out.listed += page.keys.length;
			const known = await mdb.knownRemovalKeys(this.store.pool, page.keys);
			for (const key of page.keys) {
				if (known.has(key)) continue;
				const bytes = await inv.read(key, maxRemovalBytes);
				// Records are immutable: a listed record that cannot be read leaves the window incomplete.
				if (!bytes) throw new RemovalListIncomplete("a listed removal record is missing");
				const outcome = await target.applyRestored(parseRemoval(scope, key, bytes), key);
				if (outcome !== "known") {
					out.restored++;
					this.metrics.memoryRemovals.inc({ outcome: "restored" });
				}
			}
			next = page.next;
		} while (next);
		const after = await mdb.removalWatermark(this.store.pool);
		if (after !== undefined) this.highWater = Math.max(this.highWater ?? 0, after.getTime());
		if (!this.reconciled)
			this.log.info("removal inventory reconciled: memory serves", { listed: out.listed, restored: out.restored });
		else if (out.restored > 0) this.log.warn("removals a restore erased were re-applied", { restored: out.restored });
		this.reconciled = true;
		this.lastPass = this.clock.now().getTime();
		this.metrics.memoryRemovalsReconciled.set(1);
		// Pending records do not gate memory (their decisions are in the
		// database); they are completed here when their command was not retried.
		const grace = this.bounds().pendingGraceMs ?? defaultPendingGraceMs;
		for (const row of await mdb.pendingRemovals(this.store.pool, grace, 100)) {
			try {
				await this.recordRow(row);
				out.recorded++;
			} catch (err) {
				this.log.warn("a pending removal record is not confirmed yet", {
					error: err instanceof Error ? err.name : String(err),
				});
				break;
			}
		}
		return out;
	}
}
