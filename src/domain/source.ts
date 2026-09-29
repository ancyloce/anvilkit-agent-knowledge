// The Source Registry's rules (DD-07 §1/§2): stable identities, the trusted
// ACL a command may set, readability for a scope, the media type the actual
// bytes support and the ingest input a registration freezes. Pure functions;
// the application layer runs them inside one transaction. Parsed content
// never reaches these rules: only verified identities assign access.
import { createHash } from "node:crypto";

export type PrincipalType = "tenant" | "project" | "actor" | "role";

export interface AccessEntry {
	principalType: PrincipalType;
	principalId: string;
}

export interface Scope {
	tenantId: string;
	projectId: string;
	actorId: string;
}

export interface Command {
	tenantId: string;
	commandId: string;
	actorId: string;
	requestDigest: string;
}

export type SourceKind = "document" | "url" | "repository" | "brand";

export type IngestState = "pending" | "parsing" | "indexing" | "indexed" | "failed" | "stale";

export interface Source {
	sourceId: string;
	tenantId: string;
	projectId: string;
	kind: SourceKind;
	locator: string;
	currentRevision: number;
	contentDigest: string;
	mediaType: string;
	sizeBytes: number;
	objectKey: string;
	aclRevision: number;
	access: AccessEntry[];
	ingest: IngestState;
	deleted: boolean;
	createdAt: Date;
	updatedAt: Date;
}

/** The media types the Source Registry admits (contracts/jobs mediaType). */
export const mediaTypes = [
	"application/pdf",
	"text/plain",
	"text/markdown",
	"text/html",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
] as const;
export type MediaType = (typeof mediaTypes)[number];

export class SourceError extends Error {
	constructor(
		readonly code:
			| "NOT_FOUND"
			| "INVALID_ARGUMENT"
			| "FORBIDDEN"
			| "COMMAND_CONFLICT"
			| "REVISION_MISMATCH"
			| "UNSUPPORTED_SOURCE"
			| "SOURCE_UNVERIFIED"
			| "SOURCE_TOO_LARGE",
		message: string,
	) {
		super(`${code}: ${message}`);
	}
}

const idPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const uploadName = /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/;

function hash(...parts: string[]): string {
	const h = createHash("sha256");
	for (const p of parts) h.update(p).update("\0");
	return h.digest("hex");
}

/** A registration's source id is derived from its command, so a retried create names the same source. */
export function sourceIdOf(cmd: Command): string {
	return `src-${hash("source", cmd.tenantId, cmd.commandId).slice(0, 32)}`;
}

export const ingestRequestIdOf = (sourceId: string, revision: number): string => `ing-${sourceId}-r${revision}`;
export const ingestTaskIdOf = (sourceId: string, revision: number): string => `ingest-${sourceId}-r${revision}`;

/** The caller's upload for a document source: uploads/<tenant>/<name> in Knowledge's store. */
export function uploadKeyOf(scope: Scope, locator: string): string {
	if (!locator.startsWith("upload:"))
		throw new SourceError("UNSUPPORTED_SOURCE", "a document source names an upload (upload:<name>)");
	const name = locator.slice("upload:".length);
	if (!uploadName.test(name)) throw new SourceError("INVALID_ARGUMENT", "upload name");
	return `uploads/${scope.tenantId}/${name}`;
}

/** The immutable, content-addressed copy a revision binds; the parser stages exactly this object. */
export function sourceObjectKeyOf(tenantId: string, contentDigest: string): string {
	return `sources/${tenantId}/${contentDigest.slice("sha256:".length)}`;
}

/** The command must be the scope's own (tenant and actor); never an assumed identity. */
export function checkCommand(cmd: Command, scope: Scope): void {
	if (cmd.tenantId !== scope.tenantId || cmd.actorId !== scope.actorId)
		throw new SourceError("FORBIDDEN", "command identity differs from the scope");
	if (!idPattern.test(scope.tenantId) || !idPattern.test(scope.actorId))
		throw new SourceError("INVALID_ARGUMENT", "scope identities");
	if (scope.projectId && !idPattern.test(scope.projectId)) throw new SourceError("INVALID_ARGUMENT", "project");
}

/**
 * The ACL a command may set. Principals are verified identities inside the
 * command's own scope: a tenant entry names the scope's tenant, a project
 * entry the scope's project; actor and role entries stay inside the tenant.
 * A caller cannot assign access to another tenant or project, and an empty
 * list grants the registering actor only. Entries are deduplicated and
 * ordered so equal ACLs compare equal.
 */
export function trustedAcl(scope: Scope, access: AccessEntry[]): AccessEntry[] {
	const entries = access.length > 0 ? access : [{ principalType: "actor" as const, principalId: scope.actorId }];
	const seen = new Map<string, AccessEntry>();
	for (const e of entries) {
		if (!["tenant", "project", "actor", "role"].includes(e.principalType))
			throw new SourceError("INVALID_ARGUMENT", `principal type ${e.principalType}`);
		if (!idPattern.test(e.principalId) || e.principalId.length > 128)
			throw new SourceError("INVALID_ARGUMENT", "principal id");
		if (e.principalType === "tenant" && e.principalId !== scope.tenantId)
			throw new SourceError("FORBIDDEN", "an access entry names another tenant");
		if (e.principalType === "project" && (!scope.projectId || e.principalId !== scope.projectId))
			throw new SourceError("FORBIDDEN", "an access entry names a project outside the scope");
		seen.set(`${e.principalType}:${e.principalId}`, { principalType: e.principalType, principalId: e.principalId });
	}
	return [...seen.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([, e]) => e);
}

/** The principals a scope holds: its tenant, its actor and, when set, its project. */
export function principalsOf(scope: Scope): Set<string> {
	const out = new Set([`tenant:${scope.tenantId}`, `actor:${scope.actorId}`]);
	if (scope.projectId) out.add(`project:${scope.projectId}`);
	return out;
}

/**
 * Readable now: same tenant, the scope's project when it names one (the
 * listing's rule), not deleted and the current ACL names one of the scope's
 * principals.
 */
export function readable(s: Pick<Source, "tenantId" | "projectId" | "deleted" | "access">, scope: Scope): boolean {
	if (s.deleted || s.tenantId !== scope.tenantId) return false;
	if (scope.projectId && s.projectId !== scope.projectId) return false;
	const held = principalsOf(scope);
	return s.access.some((e) => held.has(`${e.principalType}:${e.principalId}`));
}

/** Whether a replacement removes a principal that could read before (a revocation event follows). */
export function revokes(before: AccessEntry[], after: AccessEntry[]): boolean {
	const keep = new Set(after.map((e) => `${e.principalType}:${e.principalId}`));
	return before.some((e) => !keep.has(`${e.principalType}:${e.principalId}`));
}

const utf8 = new TextDecoder("utf-8", { fatal: true });

/**
 * Checks the declared media type against the actual bytes: PDF and DOCX by
 * their signatures, text formats by strict UTF-8 without NUL bytes, HTML by
 * its document marker. The parser checks structure again inside its Job.
 */
export function checkMediaType(bytes: Buffer, declared: string): MediaType {
	if (!(mediaTypes as readonly string[]).includes(declared))
		throw new SourceError("UNSUPPORTED_SOURCE", `media type ${declared}`);
	const mt = declared as MediaType;
	const mismatch = () => new SourceError("SOURCE_UNVERIFIED", `the bytes are not ${mt}`);
	switch (mt) {
		case "application/pdf":
			if (bytes.subarray(0, 5).toString("latin1") !== "%PDF-") throw mismatch();
			return mt;
		case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
			if (bytes.length < 4 || bytes.readUInt32LE(0) !== 0x04034b50) throw mismatch();
			return mt;
		default: {
			let text: string;
			try {
				text = utf8.decode(bytes);
			} catch {
				throw mismatch();
			}
			if (text.includes("\u0000")) throw mismatch();
			if (mt === "text/html") {
				// The parser's rule exactly (jobs/parser guards.py): the first
				// 4096 code points, lower-cased, contain "<!doctype html" or "<html".
				const head = Array.from(text.slice(0, 8192)).slice(0, 4096).join("").toLowerCase();
				if (!head.includes("<!doctype html") && !head.includes("<html")) throw mismatch();
			}
			return mt;
		}
	}
}

/** The frozen input of a knowledge-ingest task (the canonical JSON the claim returns). */
export interface IngestInput {
	schemaVersion: 1;
	computation: "knowledge-ingest-v1";
	sourceId: string;
	sourceRevision: number;
	ingestRequestId: string;
	parserProfile: string;
	parserProfileRevision: number;
	input: { digest: string; sizeBytes: string; mediaType: MediaType };
}

export const ingestProfile = "knowledge-ingest-v1";

export function parseIngestInput(text: string): IngestInput {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		throw new SourceError("INVALID_ARGUMENT", "ingest input is not JSON");
	}
	const r = raw as Partial<IngestInput> | null;
	const ok =
		r !== null &&
		typeof r === "object" &&
		r.schemaVersion === 1 &&
		r.computation === ingestProfile &&
		typeof r.sourceId === "string" &&
		Number.isInteger(r.sourceRevision) &&
		typeof r.ingestRequestId === "string" &&
		typeof r.parserProfile === "string" &&
		Number.isInteger(r.parserProfileRevision) &&
		typeof r.input === "object" &&
		r.input !== null &&
		/^sha256:[0-9a-f]{64}$/.test(String(r.input.digest)) &&
		/^(0|[1-9][0-9]{0,19})$/.test(String(r.input.sizeBytes)) &&
		(mediaTypes as readonly string[]).includes(String(r.input.mediaType));
	if (!ok) throw new SourceError("INVALID_ARGUMENT", "ingest input does not match knowledge-ingest-v1");
	return r as IngestInput;
}
