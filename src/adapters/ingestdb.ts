// The reviewed statements of the Source Registry and ingestion (migration
// 00003) over the owner transaction's client (contracts.md §4: one pg
// client per transaction). Readability filters run in SQL against the
// current ACL revision, so a listing never counts or pages over rows the
// scope cannot read.
import type pg from "pg";
import type { AccessEntry, IngestState, PrincipalType, Source, SourceKind } from "../domain/source.js";
import type { PoolClient } from "./postgres.js";

type Queryable = pg.Pool | PoolClient;

interface SourceRow {
	source_id: string;
	tenant_id: string;
	project_id: string;
	kind: string;
	locator: string;
	current_revision: string;
	acl_revision: string;
	deleted: boolean;
	created_at: Date;
	updated_at: Date;
	content_digest: string;
	media_type: string;
	size_bytes: string;
	object_key: string;
	ingest: string | null;
}

const sourceSelect = `SELECT s.source_id, s.tenant_id, s.project_id, s.kind, s.locator, s.current_revision::text AS current_revision,
	s.acl_revision::text AS acl_revision, s.deleted, s.created_at, s.updated_at, r.content_digest, r.media_type,
	r.size_bytes::text AS size_bytes, r.object_key,
	(SELECT i.state FROM ingest_requests i WHERE i.source_id = s.source_id AND i.source_revision = s.current_revision
	  ORDER BY i.created_at DESC LIMIT 1) AS ingest
	FROM sources s JOIN source_revisions r ON r.source_id = s.source_id AND r.revision = s.current_revision`;

async function aclOf(c: Queryable, sourceId: string, aclRevision: number): Promise<AccessEntry[]> {
	const r = await c.query<{ principal_type: string; principal_id: string }>(
		"SELECT principal_type, principal_id FROM source_acl WHERE source_id = $1 AND acl_revision = $2 ORDER BY principal_type, principal_id",
		[sourceId, aclRevision],
	);
	return r.rows.map((x) => ({ principalType: x.principal_type as PrincipalType, principalId: x.principal_id }));
}

async function fromRow(c: Queryable, r: SourceRow): Promise<Source> {
	const aclRevision = Number(r.acl_revision);
	return {
		sourceId: r.source_id,
		tenantId: r.tenant_id,
		projectId: r.project_id,
		kind: r.kind as SourceKind,
		locator: r.locator,
		currentRevision: Number(r.current_revision),
		contentDigest: r.content_digest,
		mediaType: r.media_type,
		sizeBytes: Number(r.size_bytes),
		objectKey: r.object_key,
		aclRevision,
		access: await aclOf(c, r.source_id, aclRevision),
		ingest: (r.ingest ?? "pending") as IngestState,
		deleted: r.deleted,
		createdAt: r.created_at,
		updatedAt: r.updated_at,
	};
}

export async function getSource(c: Queryable, sourceId: string, forUpdate = false): Promise<Source | undefined> {
	if (forUpdate) await c.query("SELECT 1 FROM sources WHERE source_id = $1 FOR UPDATE", [sourceId]);
	const r = await c.query<SourceRow>(`${sourceSelect} WHERE s.source_id = $1`, [sourceId]);
	return r.rows[0] ? fromRow(c, r.rows[0]) : undefined;
}

/**
 * The readable sources of a scope in (created_at, source_id) order after
 * the cursor's source (compared in the database at full timestamp precision): same tenant, not deleted, the scope's project when it has
 * one, and a current ACL entry for one of the scope's principals.
 */
export async function listReadable(
	c: Queryable,
	tenantId: string,
	projectId: string,
	principals: string[],
	afterSourceId: string,
	limit: number,
): Promise<Source[]> {
	const r = await c.query<SourceRow>(
		`${sourceSelect}
		 WHERE s.tenant_id = $1 AND NOT s.deleted AND ($2 = '' OR s.project_id = $2)
		   AND EXISTS (SELECT 1 FROM source_acl a WHERE a.source_id = s.source_id AND a.acl_revision = s.acl_revision
		               AND a.principal_type || ':' || a.principal_id = ANY($3::text[]))
		   AND ($4 = '' OR (s.created_at, s.source_id) > (SELECT c.created_at, c.source_id FROM sources c WHERE c.source_id = $4))
		 ORDER BY s.created_at, s.source_id LIMIT $5`,
		[tenantId, projectId, principals, afterSourceId, limit],
	);
	const out: Source[] = [];
	for (const row of r.rows) out.push(await fromRow(c, row));
	return out;
}

export interface CommandRecord {
	commandKind: "register" | "update_access" | "delete";
	requestDigest: string;
	sourceId: string;
}

export async function getCommand(
	c: Queryable,
	tenantId: string,
	commandId: string,
): Promise<CommandRecord | undefined> {
	const r = await c.query<{ command_kind: string; request_digest: string; source_id: string }>(
		"SELECT command_kind, request_digest, source_id FROM source_commands WHERE tenant_id = $1 AND command_id = $2",
		[tenantId, commandId],
	);
	const row = r.rows[0];
	return row
		? {
				commandKind: row.command_kind as CommandRecord["commandKind"],
				requestDigest: row.request_digest,
				sourceId: row.source_id,
			}
		: undefined;
}

export async function insertCommand(
	c: PoolClient,
	tenantId: string,
	commandId: string,
	rec: CommandRecord,
): Promise<void> {
	await c.query(
		"INSERT INTO source_commands (tenant_id, command_id, command_kind, request_digest, source_id) VALUES ($1, $2, $3, $4, $5)",
		[tenantId, commandId, rec.commandKind, rec.requestDigest, rec.sourceId],
	);
}

export async function insertSource(
	c: PoolClient,
	s: {
		sourceId: string;
		tenantId: string;
		projectId: string;
		kind: SourceKind;
		locator: string;
		commandId: string;
		requestDigest: string;
		contentDigest: string;
		mediaType: string;
		sizeBytes: number;
		objectKey: string;
	},
): Promise<void> {
	await c.query(
		`INSERT INTO sources (source_id, tenant_id, project_id, kind, locator, current_revision, acl_revision, command_id, request_digest)
		 VALUES ($1, $2, $3, $4, $5, 1, 1, $6, $7)`,
		[s.sourceId, s.tenantId, s.projectId, s.kind, s.locator, s.commandId, s.requestDigest],
	);
	await c.query(
		"INSERT INTO source_revisions (source_id, revision, content_digest, media_type, size_bytes, object_key) VALUES ($1, 1, $2, $3, $4, $5)",
		[s.sourceId, s.contentDigest, s.mediaType, s.sizeBytes, s.objectKey],
	);
}

/** Appends an ACL revision (entries plus who set it); history is never rewritten. */
export async function insertAcl(
	c: PoolClient,
	sourceId: string,
	aclRevision: number,
	entries: AccessEntry[],
	actorId: string,
	commandId: string,
): Promise<void> {
	await c.query(
		"INSERT INTO source_acl_revisions (source_id, acl_revision, actor_id, command_id) VALUES ($1, $2, $3, $4)",
		[sourceId, aclRevision, actorId, commandId],
	);
	for (const e of entries)
		await c.query(
			"INSERT INTO source_acl (source_id, acl_revision, principal_type, principal_id) VALUES ($1, $2, $3, $4)",
			[sourceId, aclRevision, e.principalType, e.principalId],
		);
}

export async function setAclRevision(c: PoolClient, sourceId: string, aclRevision: number): Promise<void> {
	await c.query("UPDATE sources SET acl_revision = $2, updated_at = now() WHERE source_id = $1", [
		sourceId,
		aclRevision,
	]);
}

export async function markDeleted(c: PoolClient, sourceId: string): Promise<void> {
	await c.query("UPDATE sources SET deleted = true, deleted_at = now(), updated_at = now() WHERE source_id = $1", [
		sourceId,
	]);
}

export interface IngestRequest {
	requestId: string;
	sourceId: string;
	sourceRevision: number;
	state: IngestState;
	parserProfile: string;
	parserProfileRevision: number;
	chunkerProfile: string;
	chunkerRevision: number;
	taskId: string;
	failureCode: string;
	pageCount: number | null;
	chunkCount: number | null;
	resultRef: string;
	resultDigest: string;
}

interface IngestRow {
	request_id: string;
	source_id: string;
	source_revision: string;
	state: string;
	parser_profile: string;
	parser_profile_revision: string;
	chunker_profile: string;
	chunker_revision: string;
	task_id: string;
	failure_code: string | null;
	page_count: number | null;
	chunk_count: number | null;
	result_ref: string | null;
	result_digest: string | null;
}

const ingestSelect = `SELECT request_id, source_id, source_revision::text AS source_revision, state, parser_profile,
	parser_profile_revision::text AS parser_profile_revision, chunker_profile, chunker_revision::text AS chunker_revision,
	task_id, failure_code, page_count, chunk_count, result_ref, result_digest FROM ingest_requests`;

function ingestFromRow(r: IngestRow): IngestRequest {
	return {
		requestId: r.request_id,
		sourceId: r.source_id,
		sourceRevision: Number(r.source_revision),
		state: r.state as IngestState,
		parserProfile: r.parser_profile,
		parserProfileRevision: Number(r.parser_profile_revision),
		chunkerProfile: r.chunker_profile,
		chunkerRevision: Number(r.chunker_revision),
		taskId: r.task_id,
		failureCode: r.failure_code ?? "",
		pageCount: r.page_count,
		chunkCount: r.chunk_count,
		resultRef: r.result_ref ?? "",
		resultDigest: r.result_digest ?? "",
	};
}

export async function insertIngestRequest(c: PoolClient, r: IngestRequest): Promise<void> {
	await c.query(
		`INSERT INTO ingest_requests (request_id, source_id, source_revision, state, parser_profile, parser_profile_revision,
		   chunker_profile, chunker_revision, task_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		[
			r.requestId,
			r.sourceId,
			r.sourceRevision,
			r.state,
			r.parserProfile,
			r.parserProfileRevision,
			r.chunkerProfile,
			r.chunkerRevision,
			r.taskId,
		],
	);
}

export async function getIngestByTask(
	c: Queryable,
	taskId: string,
	forUpdate = false,
): Promise<IngestRequest | undefined> {
	const r = await c.query<IngestRow>(`${ingestSelect} WHERE task_id = $1${forUpdate ? " FOR UPDATE" : ""}`, [taskId]);
	return r.rows[0] ? ingestFromRow(r.rows[0]) : undefined;
}

export async function updateIngest(
	c: PoolClient,
	requestId: string,
	change: Partial<
		Pick<IngestRequest, "state" | "failureCode" | "pageCount" | "chunkCount" | "resultRef" | "resultDigest">
	>,
): Promise<void> {
	await c.query(
		`UPDATE ingest_requests SET state = COALESCE($2, state), failure_code = COALESCE($3, failure_code),
		   page_count = COALESCE($4, page_count), chunk_count = COALESCE($5, chunk_count),
		   result_ref = COALESCE($6, result_ref), result_digest = COALESCE($7, result_digest), updated_at = now()
		 WHERE request_id = $1`,
		[
			requestId,
			change.state ?? null,
			change.failureCode ?? null,
			change.pageCount ?? null,
			change.chunkCount ?? null,
			change.resultRef ?? null,
			change.resultDigest ?? null,
		],
	);
}

/** Every non-terminal ingest task of a source (deletion cancels them in its transaction). */
export async function openIngestTasks(c: PoolClient, sourceId: string): Promise<string[]> {
	const r = await c.query<{ task_id: string }>(
		"SELECT task_id FROM ingest_requests WHERE source_id = $1 AND state IN ('pending', 'parsing') ORDER BY task_id",
		[sourceId],
	);
	return r.rows.map((x) => x.task_id);
}

export interface ParseLaunch {
	taskId: string;
	generation: number;
	attempt: number;
	launchKey: string;
	workerId: string;
	profileId: string;
	profileRevision: number;
	inputDigest: string;
	deadline: Date;
	state: "creating" | "running" | "completed" | "failed";
	jobUid: string;
	resultKey: string;
	resultDigest: string;
	resultSize: number | null;
	verdict: "parsed" | "rejected" | "";
	failureCode: string;
}

interface LaunchRow {
	task_id: string;
	generation: string;
	attempt: number;
	launch_key: string;
	worker_id: string;
	profile_id: string;
	profile_revision: string;
	input_digest: string;
	deadline: Date;
	state: string;
	job_uid: string | null;
	result_key: string | null;
	result_digest: string | null;
	result_size: string | null;
	verdict: string | null;
	failure_code: string | null;
}

const launchSelect = `SELECT task_id, generation::text AS generation, attempt, launch_key, worker_id, profile_id,
	profile_revision::text AS profile_revision, input_digest, deadline, state, job_uid, result_key, result_digest,
	result_size::text AS result_size, verdict, failure_code FROM parse_launches`;

function launchFromRow(r: LaunchRow): ParseLaunch {
	return {
		taskId: r.task_id,
		generation: Number(r.generation),
		attempt: r.attempt,
		launchKey: r.launch_key,
		workerId: r.worker_id,
		profileId: r.profile_id,
		profileRevision: Number(r.profile_revision),
		inputDigest: r.input_digest,
		deadline: r.deadline,
		state: r.state as ParseLaunch["state"],
		jobUid: r.job_uid ?? "",
		resultKey: r.result_key ?? "",
		resultDigest: r.result_digest ?? "",
		resultSize: r.result_size === null ? null : Number(r.result_size),
		verdict: (r.verdict ?? "") as ParseLaunch["verdict"],
		failureCode: r.failure_code ?? "",
	};
}

export async function getLaunch(
	c: Queryable,
	taskId: string,
	generation: number,
	attempt: number,
	forUpdate = false,
): Promise<ParseLaunch | undefined> {
	const r = await c.query<LaunchRow>(
		`${launchSelect} WHERE task_id = $1 AND generation = $2 AND attempt = $3${forUpdate ? " FOR UPDATE" : ""}`,
		[taskId, generation, attempt],
	);
	return r.rows[0] ? launchFromRow(r.rows[0]) : undefined;
}

/**
 * Earlier attempts' launches whose Jobs may still exist (a newer claim
 * supersedes them), including creates whose answer never arrived (no UID
 * recorded: the Job is found by its name).
 */
export async function earlierLaunches(
	c: Queryable,
	taskId: string,
	generation: number,
	attempt: number,
): Promise<ParseLaunch[]> {
	const r = await c.query<LaunchRow>(
		`${launchSelect} WHERE task_id = $1 AND generation = $2 AND attempt < $3 ORDER BY attempt`,
		[taskId, generation, attempt],
	);
	return r.rows.map(launchFromRow);
}

/**
 * Open launches nobody will observe any more: the attempt is no longer the
 * task's current leased attempt (superseded, canceled, dead, stale, a
 * failed or timed-out handler), or the deadline plus grace has passed.
 */
export async function orphanedLaunches(
	c: Queryable,
	now: Date,
	graceMs: number,
	limit: number,
): Promise<(ParseLaunch & { reason: "superseded" | "deadline" })[]> {
	const r = await c.query<LaunchRow & { reason: "superseded" | "deadline" }>(
		`SELECT l.task_id, l.generation::text AS generation, l.attempt, l.launch_key, l.worker_id, l.profile_id,
		   l.profile_revision::text AS profile_revision, l.input_digest, l.deadline, l.state, l.job_uid, l.result_key, l.result_digest,
		   l.result_size::text AS result_size, l.verdict, l.failure_code,
		   CASE WHEN r.state = 'leased' AND r.attempt_count = l.attempt THEN 'deadline' ELSE 'superseded' END AS reason
		 FROM parse_launches l JOIN background_requests r ON r.task_id = l.task_id AND r.generation = l.generation
		 WHERE l.state IN ('creating', 'running')
		   AND (NOT (r.state = 'leased' AND r.attempt_count = l.attempt)
		        OR l.deadline < $1::timestamptz - make_interval(secs => $2::float8 / 1000))
		 ORDER BY l.updated_at LIMIT $3`,
		[now, graceMs, limit],
	);
	return r.rows.map((row) => ({ ...launchFromRow(row), reason: row.reason }));
}

/** The create marker precedes the create request: state creating, create_requested. */
export async function insertLaunch(c: PoolClient, l: ParseLaunch): Promise<void> {
	await c.query(
		`INSERT INTO parse_launches (task_id, generation, attempt, launch_id, launch_key, worker_id, profile_id, profile_revision,
		   input_digest, deadline, state, create_requested)
		 VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $9, 'creating', true)`,
		[
			l.taskId,
			l.generation,
			l.attempt,
			l.launchKey,
			l.workerId,
			l.profileId,
			l.profileRevision,
			l.inputDigest,
			l.deadline,
		],
	);
}

export async function updateLaunch(
	c: Queryable,
	l: Pick<ParseLaunch, "taskId" | "generation" | "attempt">,
	change: Partial<
		Pick<ParseLaunch, "state" | "jobUid" | "resultKey" | "resultDigest" | "resultSize" | "verdict" | "failureCode">
	>,
): Promise<void> {
	await c.query(
		`UPDATE parse_launches SET state = COALESCE($4, state), job_uid = COALESCE($5, job_uid), result_key = COALESCE($6, result_key),
		   result_digest = COALESCE($7, result_digest), result_size = COALESCE($8, result_size), verdict = COALESCE($9, verdict),
		   failure_code = COALESCE($10, failure_code), updated_at = now()
		 WHERE task_id = $1 AND generation = $2 AND attempt = $3 AND state NOT IN ('completed', 'failed')`,
		[
			l.taskId,
			l.generation,
			l.attempt,
			change.state ?? null,
			change.jobUid || null,
			change.resultKey || null,
			change.resultDigest || null,
			change.resultSize ?? null,
			change.verdict || null,
			change.failureCode || null,
		],
	);
}

export interface ChunkRow {
	chunkId: string;
	sourceId: string;
	sourceRevision: number;
	parserProfile: string;
	parserProfileRevision: number;
	chunkerProfile: string;
	chunkerRevision: number;
	ordinal: number;
	locator: string;
	contentDigest: string;
	contentRef: string;
	qualityFlags: string[];
	ingestRequestId: string;
}

/** Inserts accepted chunks in bounded batches; an identical identity already present is kept. */
export async function insertChunks(c: PoolClient, rows: ChunkRow[]): Promise<void> {
	for (let i = 0; i < rows.length; i += 500) {
		const batch = rows.slice(i, i + 500);
		await c.query(
			`INSERT INTO chunks (chunk_id, source_id, source_revision, parser_profile, parser_profile_revision, chunker_profile,
			   chunker_revision, ordinal, locator, content_digest, content_ref, quality_flags, ingest_request_id)
			 SELECT chunk_id, source_id, source_revision, parser_profile, parser_profile_revision, chunker_profile, chunker_revision,
			        ordinal, locator, content_digest, content_ref, quality_flags_text::text[], ingest_request_id
			 FROM unnest($1::text[], $2::text[], $3::bigint[], $4::text[], $5::bigint[], $6::text[], $7::bigint[],
			   $8::bigint[], $9::text[], $10::text[], $11::text[], $12::text[], $13::text[])
			   AS t(chunk_id, source_id, source_revision, parser_profile, parser_profile_revision, chunker_profile, chunker_revision,
			        ordinal, locator, content_digest, content_ref, quality_flags_text, ingest_request_id)
			 ON CONFLICT (chunk_id) DO NOTHING`,
			[
				batch.map((r) => r.chunkId),
				batch.map((r) => r.sourceId),
				batch.map((r) => r.sourceRevision),
				batch.map((r) => r.parserProfile),
				batch.map((r) => r.parserProfileRevision),
				batch.map((r) => r.chunkerProfile),
				batch.map((r) => r.chunkerRevision),
				batch.map((r) => r.ordinal),
				batch.map((r) => r.locator),
				batch.map((r) => r.contentDigest),
				batch.map((r) => r.contentRef),
				batch.map((r) => `{${r.qualityFlags.join(",")}}`),
				batch.map((r) => r.ingestRequestId),
			],
		);
	}
}

export async function countChunks(c: Queryable, ingestRequestId: string): Promise<number> {
	const r = await c.query<{ n: string }>("SELECT count(*)::text AS n FROM chunks WHERE ingest_request_id = $1", [
		ingestRequestId,
	]);
	return Number(r.rows[0]?.n ?? 0);
}

/** The object a source revision binds, only when it is the given content. */
export async function revisionObject(
	c: Queryable,
	sourceId: string,
	revision: number,
	contentDigest: string,
): Promise<string | undefined> {
	const r = await c.query<{ object_key: string }>(
		"SELECT object_key FROM source_revisions WHERE source_id = $1 AND revision = $2 AND content_digest = $3",
		[sourceId, revision, contentDigest],
	);
	return r.rows[0]?.object_key;
}
