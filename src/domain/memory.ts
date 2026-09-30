// MemoryFact rules (DD-07 §5, SEC-06): identities, subjects and scopes,
// exact provenance, visibility for a reader, who may decide, the state
// transitions of a decision, the frozen input of a memory-project task and
// the manifest its application is verified by. Pure functions; the
// application layer runs them inside one transaction. Models and workers
// only propose; model confidence is never approval.
import { createHash } from "node:crypto";
import type { Scope } from "./source.js";

export type FactState = "proposed" | "confirmed" | "rejected" | "revoked" | "expired";
export type FactOrigin = "user" | "model" | "worker";
export type SubjectType = "actor" | "project" | "tenant";
export type Decision = "confirm" | "reject" | "revoke";

export interface Fact {
	factId: string;
	tenantId: string;
	subjectType: SubjectType;
	subjectId: string;
	/** The project a fact belongs to; '' for tenant-wide facts. */
	scopeId: string;
	content: string;
	contentDigest: string;
	state: FactState;
	revision: number;
	proposer: string;
	confirmer: string;
	origin: FactOrigin;
	/** Exact provenance: <source_id>@<revision>, sorted and unique. */
	sourceRefs: string[];
	expiresAt: Date | null;
	deleted: boolean;
	deletedAt: Date | null;
	purgedAt: Date | null;
	createdAt: Date;
	updatedAt: Date;
}

export class MemoryError extends Error {
	constructor(
		readonly code:
			| "NOT_FOUND"
			| "INVALID_ARGUMENT"
			| "FORBIDDEN"
			| "COMMAND_CONFLICT"
			| "REVISION_MISMATCH"
			| "INVALID_TRANSITION"
			| "PROVENANCE_STALE"
			| "FACT_CONFLICT"
			| "EXPIRED"
			| "SCOPE_TOO_LARGE"
			| "PROFILE_UNQUALIFIED"
			| "DEADLINE_EXCEEDED"
			| "UNAVAILABLE"
			| "STALE_EXECUTION",
		message: string,
	) {
		super(`${code}: ${message}`);
	}
}

const idPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const refPattern = /^([A-Za-z0-9][A-Za-z0-9._:-]{0,127})@([1-9][0-9]{0,18})$/;
export const subjectTypes: readonly SubjectType[] = ["actor", "project", "tenant"];

function sha(...parts: (string | number)[]): string {
	const h = createHash("sha256");
	for (const p of parts) h.update(String(p)).update("\0");
	return h.digest("hex");
}

/** A proposal's fact id is derived from its command, so a retried proposal names the same fact. */
export const factIdOf = (tenantId: string, commandId: string): string =>
	`mem-${sha("memory", tenantId, commandId).slice(0, 32)}`;

export const contentDigestOf = (content: string): string =>
	`sha256:${createHash("sha256").update(content, "utf8").digest("hex")}`;

export interface SourceRef {
	sourceId: string;
	revision: number;
}

export function parseSourceRef(ref: string): SourceRef {
	const m = refPattern.exec(ref);
	if (!m) throw new MemoryError("INVALID_ARGUMENT", "a source reference is <source_id>@<revision>");
	return { sourceId: m[1] as string, revision: Number(m[2]) };
}

/** Sorted, unique, parsed provenance. */
export function normalizeRefs(refs: string[]): string[] {
	for (const r of refs) parseSourceRef(r);
	return [...new Set(refs)].sort();
}

/**
 * The subject a proposal may name inside the proposer's scope: any actor of
 * the tenant, the scope's own project, or the tenant itself. A fact about
 * an actor or a project belongs to the scope's project; a tenant fact is
 * tenant-wide ('' scope).
 */
export function subjectScopeOf(scope: Scope, subjectType: string, subjectId: string): string {
	if (!(subjectTypes as readonly string[]).includes(subjectType))
		throw new MemoryError("INVALID_ARGUMENT", `subject type ${subjectType} (actor, project or tenant)`);
	if (!idPattern.test(subjectId)) throw new MemoryError("INVALID_ARGUMENT", "subject id");
	switch (subjectType as SubjectType) {
		case "actor":
			return scope.projectId;
		case "project":
			if (!scope.projectId || subjectId !== scope.projectId)
				throw new MemoryError("FORBIDDEN", "a project fact names the scope's own project");
			return scope.projectId;
		case "tenant":
			if (subjectId !== scope.tenantId) throw new MemoryError("FORBIDDEN", "a tenant fact names the scope's tenant");
			return "";
	}
}

/**
 * Readable by the scope now: same tenant, not deleted, a tenant-wide fact or
 * one of the reader's project, and a fact about an actor only for that
 * actor. The same rule decides who may decide: nobody decides on a fact
 * they cannot read.
 */
export function visible(
	f: Pick<Fact, "tenantId" | "deleted" | "scopeId" | "subjectType" | "subjectId">,
	scope: Scope,
): boolean {
	if (f.deleted || f.tenantId !== scope.tenantId) return false;
	if (f.scopeId !== "" && f.scopeId !== scope.projectId) return false;
	if (f.subjectType === "actor" && f.subjectId !== scope.actorId) return false;
	return true;
}

/** Only these facts are projected with content and recalled. */
export const recallable = (f: Pick<Fact, "state" | "deleted" | "expiresAt">, now: Date): boolean =>
	f.state === "confirmed" && !f.deleted && (f.expiresAt === null || f.expiresAt.getTime() > now.getTime());

/** The state a decision leads to from the current one; revoked, rejected and expired facts never come back. */
export function transition(from: FactState, d: Decision): FactState {
	if (from === "proposed" && d === "confirm") return "confirmed";
	if (from === "proposed" && d === "reject") return "rejected";
	if (from === "confirmed" && d === "revoke") return "revoked";
	throw new MemoryError("INVALID_TRANSITION", `a ${from} fact cannot be ${d}ed`);
}

/**
 * A decision is a user's: an identity that speaks for a model or a worker
 * never decides (neither on its own proposal nor on any other), whatever
 * confidence it reports.
 */
export function checkDecider(f: Pick<Fact, "origin" | "proposer">, decider: string, speaksForModel: boolean): void {
	if (f.origin !== "user" && f.proposer === decider)
		throw new MemoryError("FORBIDDEN", "a model or worker proposal is never decided by its proposer");
	if (speaksForModel)
		throw new MemoryError("FORBIDDEN", "an identity that proposes for a model or a worker never decides");
}

/** The reviewed rule that expires confirmed facts (a policy authority, never a user or a model). */
export const expiryPolicy = { id: "memory-expiry-v1", revision: "1" } as const;

// ---------------------------------------------------------------------------
// Projections
// ---------------------------------------------------------------------------

export const projectionProfile = "memory-project-v1";
/** Target 0 is the PostgresStore; g >= 1 the vector collection of index generation g. */
export const storeTarget = 0;
export type ProjectionAction = "apply" | "remove";

export const projectionTaskIdOf = (factId: string, target: number): string => `memproj-${factId}-t${target}`;

/** The frozen input of a memory-project task. */
export interface ProjectionInput {
	schemaVersion: 1;
	computation: typeof projectionProfile;
	factId: string;
	factRevision: number;
	target: number;
	action: ProjectionAction;
	epoch: number;
}

export function parseProjectionInput(text: string): ProjectionInput {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		throw new MemoryError("INVALID_ARGUMENT", "projection input is not JSON");
	}
	const r = raw as Partial<ProjectionInput> | null;
	const ok =
		r !== null &&
		typeof r === "object" &&
		r.schemaVersion === 1 &&
		r.computation === projectionProfile &&
		typeof r.factId === "string" &&
		Number.isSafeInteger(r.factRevision) &&
		(r.factRevision ?? 0) >= 1 &&
		Number.isSafeInteger(r.target) &&
		(r.target ?? -1) >= 0 &&
		(r.action === "apply" || r.action === "remove") &&
		Number.isSafeInteger(r.epoch) &&
		(r.epoch ?? -1) >= 0;
	if (!ok) throw new MemoryError("INVALID_ARGUMENT", `projection input does not match ${projectionProfile}`);
	return r as ProjectionInput;
}

/** The point of a fact in every vector generation: a UUID derived from the fact id. */
export function memoryPointIdOf(factId: string): string {
	const h = sha("memory-point", factId);
	return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-${(
		(Number.parseInt(h.slice(16, 18), 16) & 0x3f) |
		0x80
	)
		.toString(16)
		.padStart(2, "0")}${h.slice(18, 20)}-${h.slice(20, 32)}`;
}

/** The key recall filters on: a fact at one exact revision. */
export const memoryKeyOf = (factId: string, revision: number): string => `${factId}@${revision}`;

/** The payload of a memory point: identities and the digest, never the content. */
export interface MemoryPayload {
	kind: "memory";
	tenant_id: string;
	scope_id: string;
	subject_type: string;
	subject_id: string;
	fact_id: string;
	fact_revision: number;
	memory_key: string;
	content_digest: string;
	generation: number;
}

export const projectionResultRefOf = (factId: string, revision: number, target: number): string =>
	`memory:${target}:${memoryKeyOf(factId, revision)}`;

/** What one verified application states: the target, the fact revision, the action and the content digest. */
export function projectionDigest(p: {
	factId: string;
	revision: number;
	target: number;
	action: ProjectionAction;
	contentDigest: string;
	modelRevision: string;
}): string {
	return `sha256:${createHash("sha256")
		.update(
			`${projectionProfile}\n${p.target}\n${p.factId}\n${p.revision}\n${p.action}\n${p.action === "apply" ? p.contentDigest : ""}\n${p.modelRevision}\n`,
		)
		.digest("hex")}`;
}
