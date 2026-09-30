// The Index Builder's rules (DD-07 §3): index generations as model/content
// spaces, deterministic point identities, the frozen input of a
// knowledge-project task, the point payload Qdrant filters on and the
// manifest whose digest is the only acceptable result of that task. Pure
// functions; Qdrant is a rebuildable projection and never decides
// authorization.
import { createHash } from "node:crypto";

/** The stable alias and the physical collection of a generation (architecture.md naming). */
export const alias = "anvilkit-knowledge";
export const collectionOf = (generation: number): string => `${alias}-${generation}`;

/** The dense and sparse named vectors of every collection. */
export const vectorNames = { dense: "dense", sparse: "sparse" } as const;

/**
 * The payload fields with explicit indexes; filters use nothing else. Since
 * P17 a generation's collection also holds memory points (kind "memory",
 * no source fields): document filters name source keys, so they never match
 * a memory point, and memory filters name memory keys.
 */
export const payloadIndexes = {
	tenant_id: "keyword",
	project_id: "keyword",
	source_id: "keyword",
	source_key: "keyword",
	kind: "keyword",
	fact_id: "keyword",
	memory_key: "keyword",
} as const;

export const indexProfile = "knowledge-index-v1";

export type GenerationState = "building" | "materialized" | "accepted" | "retired";

/** One model/content space: its collection, embedding profile and model revision, sparse profile and chunker. */
export interface IndexGeneration {
	generation: number;
	modelId: string;
	modelRevision: string;
	embeddingProfile: string;
	dimensions: number;
	sparseProfile: string;
	chunkerProfile: string;
	chunkerRevision: number;
	collectionName: string;
	state: GenerationState;
	expectedPoints: number;
	verifiedPoints: number | null;
	acceptedAt: Date | null;
}

/** The profile a new generation is built for: the reviewed embedding profile plus the parser's chunker. */
export interface SpaceProfile {
	modelId: string;
	modelRevision: string;
	embeddingProfile: string;
	dimensions: number;
	sparseProfile: string;
	chunkerProfile: string;
	chunkerRevision: number;
}

/** A generation serves a profile only when every binding of its vector space is equal. */
export function sameSpace(g: Omit<SpaceProfile, "chunkerProfile" | "chunkerRevision">, p: SpaceProfile): boolean {
	return (
		g.modelId === p.modelId &&
		g.modelRevision === p.modelRevision &&
		g.embeddingProfile === p.embeddingProfile &&
		g.dimensions === p.dimensions &&
		g.sparseProfile === p.sparseProfile
	);
}

export type EntryState = "pending" | "running" | "materialized" | "accepted" | "failed" | "stale";

/** The state of one source revision in one generation. */
export interface IndexEntry {
	generation: number;
	sourceId: string;
	sourceRevision: number;
	ingestRequestId: string;
	taskId: string;
	state: EntryState;
	chunkCount: number;
	writtenThrough: number;
	pointCount: number | null;
	manifestDigest: string;
	failureCode: string;
}

export class IndexError extends Error {
	constructor(
		readonly code: "NOT_FOUND" | "INVALID_ARGUMENT" | "STALE_EXECUTION" | "UNAVAILABLE",
		message: string,
	) {
		super(`${code}: ${message}`);
	}
}

export const indexFailure = {
	unavailable: "INDEX_UNAVAILABLE",
	profileUnqualified: "PROFILE_UNQUALIFIED",
	generationRetired: "GENERATION_RETIRED",
	authorizationRevoked: "AUTHORIZATION_REVOKED",
	chunkUnverified: "CHUNK_UNVERIFIED",
	computeRefused: "COMPUTE_REFUSED",
} as const;

function sha(...parts: (string | number)[]): string {
	const h = createHash("sha256");
	for (const p of parts) h.update(String(p)).update("\0");
	return h.digest("hex");
}

export const indexTaskIdOf = (sourceId: string, revision: number, generation: number): string =>
	`index-${sourceId}-r${revision}-g${generation}`;

export const sourceKeyOf = (sourceId: string, revision: number): string => `${sourceId}@${revision}`;

/**
 * The point id of a chunk: a UUID derived from the chunk identity, which
 * already binds source revision, parser/chunker revisions and ordinal. A
 * rewrite after a partial or unanswered upsert therefore names the same
 * points; nothing can accumulate duplicates.
 */
export function pointIdOf(chunkId: string): string {
	const h = sha("point", chunkId);
	return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-${(
		(Number.parseInt(h.slice(16, 18), 16) & 0x3f) |
		0x80
	)
		.toString(16)
		.padStart(2, "0")}${h.slice(18, 20)}-${h.slice(20, 32)}`;
}

/** The payload of a point: identities and the content digest only, never the text. */
export interface PointPayload {
	tenant_id: string;
	project_id: string;
	source_id: string;
	source_revision: number;
	source_key: string;
	chunk_id: string;
	ordinal: number;
	content_digest: string;
	generation: number;
}

/** The frozen input of a knowledge-project task (the canonical JSON the claim returns). */
export interface IndexInput {
	schemaVersion: 1;
	computation: typeof indexProfile;
	sourceId: string;
	sourceRevision: number;
	ingestRequestId: string;
	indexGeneration: number;
	chunkCount: number;
	embeddingProfile: string;
	modelRevision: string;
}

export function parseIndexInput(text: string): IndexInput {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		throw new IndexError("INVALID_ARGUMENT", "index input is not JSON");
	}
	const r = raw as Partial<IndexInput> | null;
	const ok =
		r !== null &&
		typeof r === "object" &&
		r.schemaVersion === 1 &&
		r.computation === indexProfile &&
		typeof r.sourceId === "string" &&
		Number.isSafeInteger(r.sourceRevision) &&
		typeof r.ingestRequestId === "string" &&
		Number.isSafeInteger(r.indexGeneration) &&
		(r.indexGeneration ?? 0) >= 1 &&
		Number.isSafeInteger(r.chunkCount) &&
		(r.chunkCount ?? -1) >= 0 &&
		typeof r.embeddingProfile === "string" &&
		typeof r.modelRevision === "string";
	if (!ok) throw new IndexError("INVALID_ARGUMENT", `index input does not match ${indexProfile}`);
	return r as IndexInput;
}

export const indexResultRefOf = (generation: number, sourceId: string, revision: number): string =>
	`index:${generation}:${sourceKeyOf(sourceId, revision)}`;

/** One manifest line per point in ordinal order, bound to the generation, source revision and model revision. */
export function manifestDigest(
	g: Pick<IndexGeneration, "generation" | "modelRevision" | "sparseProfile">,
	sourceKey: string,
	points: { pointId: string; chunkId: string; contentDigest: string }[],
): string {
	const h = createHash("sha256");
	h.update(
		`${indexProfile}\n${g.generation}\n${g.modelRevision}\n${g.sparseProfile}\n${sourceKey}\n${points.length}\n`,
	);
	for (const p of points) h.update(`${p.pointId} ${p.chunkId} ${p.contentDigest}\n`);
	return `sha256:${h.digest("hex")}`;
}

/** Why a stored point does not prove the chunk it names (undefined when it does). */
export function pointMismatch(
	stored: { payload?: Record<string, unknown> | null; hasDense: boolean; hasSparse: boolean } | undefined,
	want: PointPayload,
): string | undefined {
	if (!stored) return "missing";
	if (!stored.hasDense || !stored.hasSparse) return "vector missing";
	const p = stored.payload ?? {};
	for (const [k, v] of Object.entries(want)) if (p[k] !== v) return `payload ${k}`;
	return undefined;
}

/**
 * A building generation is materialized when every eligible source
 * revision has an accepted entry and the collection holds exactly the
 * ledger's points (the watermark); anything else keeps it building.
 */
export function qualifies(p: {
	eligible: number;
	accepted: number;
	open: number;
	expectedPoints: number;
	countedPoints: number;
}): { ok: true } | { ok: false; reason: string } {
	if (p.open > 0) return { ok: false, reason: `${p.open} entries not accepted` };
	if (p.accepted !== p.eligible)
		return { ok: false, reason: `${p.accepted} of ${p.eligible} source revisions indexed` };
	if (p.countedPoints !== p.expectedPoints)
		return { ok: false, reason: `${p.countedPoints} points counted, ${p.expectedPoints} in the ledger` };
	return { ok: true };
}
