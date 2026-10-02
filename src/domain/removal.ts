// Memory removal records (P23; execution.md §4 recovery, SEC-06): the
// immutable evidence of one deletion or revocation that lives outside the
// business database. Its key names the database's removal scope (the
// inventory belongs to that database alone; a restore keeps the scope),
// then orders records by the database time the decision committed under,
// so the window a restore may have erased is a key range; the hash part
// binds the (tenant, command) identity the decision was taken under. Only
// identities, revisions and digests are recorded: never fact content.
// Pure functions.
import { createHash } from "node:crypto";
import type { Fact } from "./memory.js";

const scopePattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** The key prefix of one database's records. */
export function removalPrefixOf(scope: string): string {
	if (!scopePattern.test(scope)) throw new RemovalError(`removal scope ${scope} is not a UUID`);
	return `removals/${scope}/`;
}
/** The read bound of one record (records are a few hundred bytes). */
export const maxRemovalBytes = 16_384;

export type RemovalDecision = "delete" | "revoke";

export interface RemovalRecord {
	schemaVersion: 1;
	kind: "memory-removal";
	tenantId: string;
	commandId: string;
	requestDigest: string;
	factId: string;
	decision: RemovalDecision;
	/** The user who took the decision. */
	decider: string;
	/** A revocation's confirmer (a restore may have erased the confirmation too); '' for a deletion. */
	confirmer: string;
	reasonCode: string;
	fromRevision: number;
	toRevision: number;
	/** The database time of the decision, UTC with milliseconds. */
	recordedAt: string;
}

const fields = [
	"schemaVersion",
	"kind",
	"tenantId",
	"commandId",
	"requestDigest",
	"factId",
	"decision",
	"decider",
	"confirmer",
	"reasonCode",
	"fromRevision",
	"toRevision",
	"recordedAt",
] as const;

export class RemovalError extends Error {}

/** 2026-10-02T15:01:12.123Z → 20261002T150112.123Z (fixed width, so key order is time order). */
function compactTime(iso: string): string {
	const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z$/.exec(iso);
	if (!m) throw new RemovalError(`recordedAt ${iso} is not UTC with milliseconds`);
	return `${m[1]}${m[2]}${m[3]}T${m[4]}${m[5]}${m[6]}.${m[7]}Z`;
}

/** The first key of the scope's records at or after `at`. */
export function removalKeyFloor(scope: string, at: Date): string {
	return `${removalPrefixOf(scope)}${compactTime(at.toISOString())}/`;
}

export function removalKeyOf(scope: string, r: Pick<RemovalRecord, "tenantId" | "commandId" | "recordedAt">): string {
	const id = createHash("sha256").update(r.tenantId).update("\0").update(r.commandId).digest("hex").slice(0, 32);
	return `${removalPrefixOf(scope)}${compactTime(r.recordedAt)}/${id}.json`;
}

/** The record's bytes: its fields in the fixed order, so a retried create compares equal. */
export function removalBodyOf(r: RemovalRecord): Buffer {
	const ordered: Record<string, unknown> = {};
	for (const k of fields) ordered[k] = r[k];
	return Buffer.from(JSON.stringify(ordered), "utf8");
}

const bounded = (v: unknown, max = 1024): v is string => typeof v === "string" && v.length <= max;

/** Parses the bytes listed under `key`; anything but a well-formed record bound to that key is refused. */
export function parseRemoval(scope: string, key: string, bytes: Buffer): RemovalRecord {
	let raw: unknown;
	try {
		raw = JSON.parse(bytes.toString("utf8"));
	} catch {
		throw new RemovalError("a removal record is not JSON");
	}
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) throw new RemovalError("not an object");
	const r = raw as Record<string, unknown>;
	const keys = Object.keys(r);
	if (keys.length !== fields.length || !fields.every((f) => f in r)) throw new RemovalError("unexpected fields");
	const ok =
		r.schemaVersion === 1 &&
		r.kind === "memory-removal" &&
		bounded(r.tenantId) &&
		r.tenantId !== "" &&
		bounded(r.commandId) &&
		r.commandId !== "" &&
		bounded(r.requestDigest) &&
		r.requestDigest !== "" &&
		typeof r.factId === "string" &&
		/^mem-[0-9a-f]{32}$/.test(r.factId) &&
		(r.decision === "delete" || r.decision === "revoke") &&
		bounded(r.decider) &&
		r.decider !== "" &&
		bounded(r.confirmer) &&
		(r.decision === "revoke") === (r.confirmer !== "") &&
		bounded(r.reasonCode, 128) &&
		Number.isSafeInteger(r.fromRevision) &&
		(r.fromRevision as number) >= 1 &&
		r.toRevision === (r.fromRevision as number) + 1 &&
		typeof r.recordedAt === "string" &&
		!Number.isNaN(Date.parse(r.recordedAt)) &&
		new Date(r.recordedAt).toISOString() === r.recordedAt;
	if (!ok) throw new RemovalError("a removal record field is malformed");
	const rec = r as unknown as RemovalRecord;
	if (removalKeyOf(scope, rec) !== key) throw new RemovalError("a removal record is not bound to its key");
	if (!removalBodyOf(rec).equals(bytes)) throw new RemovalError("a removal record is not in its canonical form");
	return rec;
}

/**
 * The fact a restored database must hold once a recorded removal is
 * re-applied, or undefined when it already holds it. A deletion erases the
 * content of any fact not yet deleted, at the time it was taken. A
 * revocation ends a confirmed fact; if the restore erased the confirmation
 * as well, the proposal still becomes revoked (with the recorded confirmer)
 * so no retried confirmation revives what the user revoked. A deleted,
 * rejected, revoked or expired fact needs nothing.
 */
export function restoredRemoval(f: Fact, r: RemovalRecord): Fact | undefined {
	if (f.deleted) return undefined;
	if (r.decision === "delete")
		return { ...f, revision: f.revision + 1, deleted: true, deletedAt: new Date(r.recordedAt), content: "" };
	if (f.state === "confirmed") return { ...f, state: "revoked", revision: f.revision + 1 };
	if (f.state === "proposed") return { ...f, state: "revoked", confirmer: r.confirmer, revision: f.revision + 1 };
	return undefined;
}
