// The reviewed statements of index generations, the index ledger and the
// deletion purge (migration 00004) over the owner transaction's client
// (contracts.md §4: one pg client per transaction). Eligibility counts only
// current revisions of sources that are not deleted, so a deleted source can
// neither block nor count toward a generation's qualification.
import type pg from "pg";
import type { EntryState, GenerationState, IndexEntry, IndexGeneration, SpaceProfile } from "../domain/index.js";
import type { ChunkRow } from "./ingestdb.js";
import type { PoolClient } from "./postgres.js";

type Queryable = pg.Pool | PoolClient;

interface GenerationRow {
	generation: string;
	model_id: string;
	model_revision: string;
	embedding_profile: string;
	dimensions: number;
	sparse_profile: string;
	chunker_profile: string;
	chunker_revision: string;
	collection_name: string;
	state: string;
	expected_points: string;
	verified_points: string | null;
	accepted_at: Date | null;
}

const generationSelect = `SELECT generation::text AS generation, model_id, model_revision, embedding_profile, dimensions,
	sparse_profile, chunker_profile, chunker_revision::text AS chunker_revision, collection_name, state,
	expected_points::text AS expected_points, verified_points::text AS verified_points, accepted_at FROM index_generations`;

function generationFromRow(r: GenerationRow): IndexGeneration {
	return {
		generation: Number(r.generation),
		modelId: r.model_id,
		modelRevision: r.model_revision,
		embeddingProfile: r.embedding_profile,
		dimensions: r.dimensions,
		sparseProfile: r.sparse_profile,
		chunkerProfile: r.chunker_profile,
		chunkerRevision: Number(r.chunker_revision),
		collectionName: r.collection_name,
		state: r.state as GenerationState,
		expectedPoints: Number(r.expected_points),
		verifiedPoints: r.verified_points === null ? null : Number(r.verified_points),
		acceptedAt: r.accepted_at,
	};
}

export async function listGenerations(c: Queryable): Promise<IndexGeneration[]> {
	const r = await c.query<GenerationRow>(`${generationSelect} ORDER BY generation`);
	return r.rows.map(generationFromRow);
}

export async function getGeneration(
	c: Queryable,
	generation: number,
	forUpdate = false,
): Promise<IndexGeneration | undefined> {
	const r = await c.query<GenerationRow>(`${generationSelect} WHERE generation = $1${forUpdate ? " FOR UPDATE" : ""}`, [
		generation,
	]);
	return r.rows[0] ? generationFromRow(r.rows[0]) : undefined;
}

/** The next generation number, recorded as building; a concurrent creator makes this insert fail (unique). */
export async function insertGeneration(
	c: PoolClient,
	p: SpaceProfile,
	collectionOf: (n: number) => string,
): Promise<number> {
	const next = await c.query<{ n: string }>(
		"SELECT (COALESCE(max(generation), 0) + 1)::text AS n FROM index_generations",
	);
	const n = Number(next.rows[0]?.n ?? 1);
	await c.query(
		`INSERT INTO index_generations (generation, model_id, model_revision, chunker_profile, collection_name, state,
		   embedding_profile, dimensions, sparse_profile, chunker_revision)
		 VALUES ($1, $2, $3, $4, $5, 'building', $6, $7, $8, $9)`,
		[
			n,
			p.modelId,
			p.modelRevision,
			p.chunkerProfile,
			collectionOf(n),
			p.embeddingProfile,
			p.dimensions,
			p.sparseProfile,
			p.chunkerRevision,
		],
	);
	return n;
}

export async function markMaterialized(c: PoolClient, generation: number, points: number): Promise<void> {
	await c.query(
		`UPDATE index_generations SET state = 'materialized', expected_points = $2, verified_points = $2, materialized_at = now()
		 WHERE generation = $1 AND state = 'building'`,
		[generation, points],
	);
}

/** The accepted generation retires and the materialized one becomes the alias target, in one transaction. */
export async function acceptGeneration(c: PoolClient, generation: number): Promise<void> {
	await c.query(
		"UPDATE index_generations SET state = 'retired', retired_at = now() WHERE state = 'accepted' AND generation <> $1",
		[generation],
	);
	await c.query(
		"UPDATE index_generations SET state = 'accepted', accepted_at = now() WHERE generation = $1 AND state = 'materialized'",
		[generation],
	);
}

export async function reopenGeneration(c: PoolClient, generation: number): Promise<void> {
	await c.query(
		"UPDATE index_generations SET state = 'building', verified_points = NULL, materialized_at = NULL WHERE generation = $1 AND state = 'materialized'",
		[generation],
	);
}

interface EntryRow {
	generation: string;
	source_id: string;
	source_revision: string;
	ingest_request_id: string;
	task_id: string;
	state: string;
	chunk_count: number;
	written_through: number;
	point_count: number | null;
	manifest_digest: string | null;
	failure_code: string | null;
}

const entrySelect = `SELECT generation::text AS generation, source_id, source_revision::text AS source_revision, ingest_request_id,
	task_id, state, chunk_count, written_through, point_count, manifest_digest, failure_code FROM index_entries`;

function entryFromRow(r: EntryRow): IndexEntry {
	return {
		generation: Number(r.generation),
		sourceId: r.source_id,
		sourceRevision: Number(r.source_revision),
		ingestRequestId: r.ingest_request_id,
		taskId: r.task_id,
		state: r.state as EntryState,
		chunkCount: r.chunk_count,
		writtenThrough: r.written_through,
		pointCount: r.point_count,
		manifestDigest: r.manifest_digest ?? "",
		failureCode: r.failure_code ?? "",
	};
}

/** Records an entry once; an existing entry of the same generation and revision is kept. */
export async function insertEntry(
	c: PoolClient,
	e: Pick<IndexEntry, "generation" | "sourceId" | "sourceRevision" | "ingestRequestId" | "taskId" | "chunkCount">,
): Promise<boolean> {
	const r = await c.query(
		`INSERT INTO index_entries (generation, source_id, source_revision, ingest_request_id, task_id, state, chunk_count)
		 VALUES ($1, $2, $3, $4, $5, 'pending', $6) ON CONFLICT DO NOTHING`,
		[e.generation, e.sourceId, e.sourceRevision, e.ingestRequestId, e.taskId, e.chunkCount],
	);
	return (r.rowCount ?? 0) === 1;
}

export async function getEntryByTask(c: Queryable, taskId: string, forUpdate = false): Promise<IndexEntry | undefined> {
	const r = await c.query<EntryRow>(`${entrySelect} WHERE task_id = $1${forUpdate ? " FOR UPDATE" : ""}`, [taskId]);
	return r.rows[0] ? entryFromRow(r.rows[0]) : undefined;
}

/** Entries of source revisions in one generation. */
export async function acceptedEntries(
	c: Queryable,
	generation: number,
	revisions: { sourceId: string; revision: number }[],
): Promise<IndexEntry[]> {
	if (revisions.length === 0) return [];
	const r = await c.query<EntryRow>(
		`${entrySelect} WHERE generation = $1 AND state = 'accepted'
		   AND (source_id, source_revision) IN (SELECT * FROM unnest($2::text[], $3::bigint[]))`,
		[generation, revisions.map((x) => x.sourceId), revisions.map((x) => x.revision)],
	);
	return r.rows.map(entryFromRow);
}

/**
 * Moves an entry of the given task. A progress update binds the watermark it
 * read, so two calls racing on one claim cannot move it backwards or skip a batch.
 */
export async function updateEntry(
	c: Queryable,
	taskId: string,
	change: Partial<Pick<IndexEntry, "state" | "writtenThrough" | "pointCount" | "manifestDigest" | "failureCode">>,
	readWrittenThrough?: number,
): Promise<number> {
	const r = await c.query(
		`UPDATE index_entries SET state = COALESCE($2, state), written_through = COALESCE($3, written_through),
		   point_count = COALESCE($4, point_count), manifest_digest = COALESCE($5, manifest_digest),
		   failure_code = COALESCE($6, failure_code), updated_at = now()
		 WHERE task_id = $1 AND ($7::int IS NULL OR written_through = $7)`,
		[
			taskId,
			change.state ?? null,
			change.writtenThrough ?? null,
			change.pointCount ?? null,
			change.manifestDigest || null,
			change.failureCode || null,
			readWrittenThrough ?? null,
		],
	);
	return r.rowCount ?? 0;
}

/** The accepted chunks of an ingest request in ordinal order, a page at a time. */
export async function chunkPage(
	c: Queryable,
	ingestRequestId: string,
	fromOrdinal: number,
	limit: number,
): Promise<ChunkRow[]> {
	const r = await c.query<{
		chunk_id: string;
		source_id: string;
		source_revision: string;
		parser_profile: string;
		parser_profile_revision: string;
		chunker_profile: string;
		chunker_revision: string;
		ordinal: string;
		locator: string;
		content_digest: string;
		content_ref: string;
		quality_flags: string[];
		ingest_request_id: string;
	}>(
		`SELECT chunk_id, source_id, source_revision::text AS source_revision, parser_profile,
		   parser_profile_revision::text AS parser_profile_revision, chunker_profile, chunker_revision::text AS chunker_revision,
		   ordinal::text AS ordinal, locator, content_digest, content_ref, quality_flags, ingest_request_id
		 FROM chunks WHERE ingest_request_id = $1 AND ordinal >= $2 ORDER BY ordinal LIMIT $3`,
		[ingestRequestId, fromOrdinal, limit],
	);
	return r.rows.map((x) => ({
		chunkId: x.chunk_id,
		sourceId: x.source_id,
		sourceRevision: Number(x.source_revision),
		parserProfile: x.parser_profile,
		parserProfileRevision: Number(x.parser_profile_revision),
		chunkerProfile: x.chunker_profile,
		chunkerRevision: Number(x.chunker_revision),
		ordinal: Number(x.ordinal),
		locator: x.locator,
		contentDigest: x.content_digest,
		contentRef: x.content_ref,
		qualityFlags: x.quality_flags,
		ingestRequestId: x.ingest_request_id,
	}));
}

/** The chunks with the given ids (retrieval reads only these). */
export async function chunksById(c: Queryable, chunkIds: string[]): Promise<Map<string, ChunkRow>> {
	const out = new Map<string, ChunkRow>();
	if (chunkIds.length === 0) return out;
	const r = await c.query<{
		chunk_id: string;
		source_id: string;
		source_revision: string;
		ordinal: string;
		locator: string;
		content_digest: string;
		content_ref: string;
		quality_flags: string[];
		ingest_request_id: string;
		parser_profile: string;
		parser_profile_revision: string;
		chunker_profile: string;
		chunker_revision: string;
	}>(
		`SELECT chunk_id, source_id, source_revision::text AS source_revision, ordinal::text AS ordinal, locator, content_digest,
		   content_ref, quality_flags, ingest_request_id, parser_profile, parser_profile_revision::text AS parser_profile_revision,
		   chunker_profile, chunker_revision::text AS chunker_revision
		 FROM chunks WHERE chunk_id = ANY($1::text[])`,
		[chunkIds],
	);
	for (const x of r.rows)
		out.set(x.chunk_id, {
			chunkId: x.chunk_id,
			sourceId: x.source_id,
			sourceRevision: Number(x.source_revision),
			parserProfile: x.parser_profile,
			parserProfileRevision: Number(x.parser_profile_revision),
			chunkerProfile: x.chunker_profile,
			chunkerRevision: Number(x.chunker_revision),
			ordinal: Number(x.ordinal),
			locator: x.locator,
			contentDigest: x.content_digest,
			contentRef: x.content_ref,
			qualityFlags: x.quality_flags,
			ingestRequestId: x.ingest_request_id,
		});
	return out;
}

export interface EligibleRevision {
	sourceId: string;
	sourceRevision: number;
	tenantId: string;
	ingestRequestId: string;
	chunkCount: number;
}

/**
 * Parsed current revisions of sources that are not deleted, whose chunks
 * were made by the given chunker: the revisions a generation must index.
 */
export async function eligibleRevisions(
	c: Queryable,
	chunkerProfile: string,
	chunkerRevision: number,
	afterRequestId: string,
	limit: number,
): Promise<EligibleRevision[]> {
	const r = await c.query<{
		source_id: string;
		source_revision: string;
		tenant_id: string;
		request_id: string;
		chunk_count: number;
	}>(
		`SELECT i.source_id, i.source_revision::text AS source_revision, s.tenant_id, i.request_id, i.chunk_count
		 FROM ingest_requests i JOIN sources s ON s.source_id = i.source_id AND s.current_revision = i.source_revision
		 WHERE NOT s.deleted AND i.state IN ('indexing', 'indexed') AND i.chunk_count IS NOT NULL
		   AND i.chunker_profile = $1 AND i.chunker_revision = $2 AND i.request_id > $3
		 ORDER BY i.request_id LIMIT $4`,
		[chunkerProfile, chunkerRevision, afterRequestId, limit],
	);
	return r.rows.map((x) => ({
		sourceId: x.source_id,
		sourceRevision: Number(x.source_revision),
		tenantId: x.tenant_id,
		ingestRequestId: x.request_id,
		chunkCount: x.chunk_count,
	}));
}

export interface Progress {
	eligible: number;
	accepted: number;
	open: number;
	failed: number;
	expectedPoints: number;
}

/** What a building generation still lacks, counted over eligible revisions only. */
export async function progressOf(c: Queryable, g: IndexGeneration): Promise<Progress> {
	const r = await c.query<{ eligible: string; accepted: string; open: string; failed: string; points: string }>(
		`WITH eligible AS (
		   SELECT i.source_id, i.source_revision FROM ingest_requests i
		   JOIN sources s ON s.source_id = i.source_id AND s.current_revision = i.source_revision
		   WHERE NOT s.deleted AND i.state IN ('indexing', 'indexed') AND i.chunk_count IS NOT NULL
		     AND i.chunker_profile = $2 AND i.chunker_revision = $3)
		 SELECT (SELECT count(*) FROM eligible)::text AS eligible,
		   count(*) FILTER (WHERE e.state = 'accepted')::text AS accepted,
		   count(*) FILTER (WHERE e.state IN ('pending', 'running', 'materialized'))::text AS open,
		   count(*) FILTER (WHERE e.state = 'failed')::text AS failed,
		   COALESCE(sum(e.point_count) FILTER (WHERE e.state = 'accepted'), 0)::text AS points
		 FROM index_entries e JOIN eligible x ON x.source_id = e.source_id AND x.source_revision = e.source_revision
		 WHERE e.generation = $1`,
		[g.generation, g.chunkerProfile, g.chunkerRevision],
	);
	const x = r.rows[0];
	return {
		eligible: Number(x?.eligible ?? 0),
		accepted: Number(x?.accepted ?? 0),
		open: Number(x?.open ?? 0),
		failed: Number(x?.failed ?? 0),
		expectedPoints: Number(x?.points ?? 0),
	};
}

/** Every non-terminal index entry's task of a source (deletion cancels them in its transaction). */
export async function openIndexTasks(c: PoolClient, sourceId: string): Promise<string[]> {
	const r = await c.query<{ task_id: string }>(
		"SELECT task_id FROM index_entries WHERE source_id = $1 AND state IN ('pending', 'running', 'materialized') ORDER BY task_id",
		[sourceId],
	);
	return r.rows.map((x) => x.task_id);
}

export async function staleEntries(c: PoolClient, sourceId: string): Promise<void> {
	await c.query(
		"UPDATE index_entries SET state = 'stale', failure_code = 'SOURCE_DELETED', updated_at = now() WHERE source_id = $1 AND state <> 'stale'",
		[sourceId],
	);
}

/** Deleted sources whose points may still exist in some generation. */
export async function unpurged(c: Queryable, limit: number): Promise<string[]> {
	const r = await c.query<{ source_id: string }>(
		"SELECT source_id FROM sources WHERE deleted AND points_purged_at IS NULL ORDER BY deleted_at LIMIT $1",
		[limit],
	);
	return r.rows.map((x) => x.source_id);
}

export async function markPurged(c: Queryable, sourceId: string): Promise<void> {
	await c.query("UPDATE sources SET points_purged_at = now() WHERE source_id = $1 AND deleted", [sourceId]);
}
