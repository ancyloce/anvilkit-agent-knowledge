// Snapshots and retrieval (DD-07 §4). A snapshot freezes exact source
// revisions and the index generation they were read from; it never freezes
// permission: every search resolves the current ACL again. These are pure
// rules; the application layer runs the reads and rechecks.
import { createHash } from "node:crypto";

export class RetrievalError extends Error {
	constructor(
		readonly code:
			| "NOT_FOUND"
			| "INVALID_ARGUMENT"
			| "FORBIDDEN"
			| "COMMAND_CONFLICT"
			| "NOT_INDEXED"
			| "PROFILE_UNQUALIFIED"
			| "SCOPE_TOO_LARGE"
			| "DEADLINE_EXCEEDED"
			| "UNAVAILABLE",
		message: string,
	) {
		super(`${code}: ${message}`);
	}
}

export interface SnapshotSource {
	sourceId: string;
	sourceRevision: number;
	contentDigest: string;
}

export interface Snapshot {
	snapshotId: string;
	tenantId: string;
	projectId: string;
	generation: number;
	sources: SnapshotSource[];
	contentDigest: string;
	requestDigest: string;
	createdAt: Date;
}

function sha(...parts: (string | number)[]): string {
	const h = createHash("sha256");
	for (const p of parts) h.update(String(p)).update("\0");
	return h.digest("hex");
}

/** A retried create names the same snapshot. */
export const snapshotIdOf = (tenantId: string, commandId: string): string =>
	`snap-${sha("snapshot", tenantId, commandId).slice(0, 32)}`;

/** The frozen content: generation plus every revision's content digest, in source order. */
export function snapshotDigest(generation: number, sources: SnapshotSource[]): string {
	const lines = [...sources]
		.sort((a, b) => (a.sourceId < b.sourceId ? -1 : a.sourceId > b.sourceId ? 1 : 0))
		.map((s) => `${s.sourceId}@${s.sourceRevision} ${s.contentDigest}`);
	return `sha256:${sha("snapshot-content", generation, ...lines)}`;
}

/**
 * The digest of the authorization a search used: every allowed source with
 * its current ACL revision. Two searches under the same ACL state share it;
 * any grant or revocation changes it.
 */
export function authorizationRevisionOf(allowed: { sourceId: string; aclRevision: number }[]): string {
	const lines = [...allowed]
		.sort((a, b) => (a.sourceId < b.sourceId ? -1 : a.sourceId > b.sourceId ? 1 : 0))
		.map((a) => `${a.sourceId}#${a.aclRevision}`);
	return `sha256:${sha("authorization", ...lines)}`;
}

/** The reviewed bounds of one retrieval profile (DEVELOPMENT_ONLY values in this build). */
export interface RetrievalProfile {
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
}

/** Fused candidates in score order with a stable tie-break on the chunk id. */
export function stableOrder<T extends { score: number; chunkId: string }>(xs: T[]): T[] {
	return [...xs].sort((a, b) =>
		b.score !== a.score ? b.score - a.score : a.chunkId < b.chunkId ? -1 : a.chunkId > b.chunkId ? 1 : 0,
	);
}

export interface Candidate {
	chunkId: string;
	sourceId: string;
	sourceRevision: number;
	locator: string;
	contentDigest: string;
	text: string;
	score: number;
}

export interface ContextItem {
	citation: {
		ordinal: string;
		sourceId: string;
		sourceRevision: string;
		chunkId: string;
		locator: string;
		contentDigest: string;
		score: string;
	};
	text: string;
}

/** A score as a bounded decimal string (six places), never an exponent. */
export function scoreText(x: number): string {
	return x.toFixed(6);
}

/**
 * The context: reranked candidates above the profile's floor in stable
 * order, duplicates of the same content dropped, whole items only within
 * the character budget (code points) and the item bound, numbered 1..n.
 * No citation is produced for text that is not returned.
 */
export function assembleContext(
	ranked: Candidate[],
	maxItems: number,
	maxChars: number,
	minScore: number,
): ContextItem[] {
	const out: ContextItem[] = [];
	const seen = new Set<string>();
	let chars = 0;
	for (const c of stableOrder(ranked)) {
		if (out.length >= maxItems) break;
		if (c.score < minScore) break;
		if (seen.has(c.contentDigest)) continue;
		const n = Array.from(c.text).length;
		if (chars + n > maxChars) continue;
		seen.add(c.contentDigest);
		chars += n;
		out.push({
			citation: {
				ordinal: String(out.length + 1),
				sourceId: c.sourceId,
				sourceRevision: String(c.sourceRevision),
				chunkId: c.chunkId,
				locator: c.locator,
				contentDigest: c.contentDigest,
				score: scoreText(c.score),
			},
			text: c.text,
		});
	}
	return out;
}
