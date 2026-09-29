// The reviewed statements of snapshots and of retrieval's authorization
// reads (DD-07 §4). readableNow is the Source Registry's readability rule
// (same tenant, the scope's project when it names one, not deleted, a
// current ACL entry for one of the scope's principals) evaluated in SQL for
// a set of sources at once; it is the authority every search and every
// recheck uses. Nothing here reads Qdrant.
import type pg from "pg";
import type { Snapshot, SnapshotSource } from "../domain/retrieval.js";
import type { PoolClient } from "./postgres.js";

type Queryable = pg.Pool | PoolClient;

export interface Readable {
	sourceId: string;
	projectId: string;
	currentRevision: number;
	aclRevision: number;
	contentDigest: string;
}

export async function readableNow(
	c: Queryable,
	tenantId: string,
	projectId: string,
	principals: string[],
	sourceIds: string[],
): Promise<Map<string, Readable>> {
	const out = new Map<string, Readable>();
	if (sourceIds.length === 0) return out;
	const r = await c.query<{
		source_id: string;
		project_id: string;
		current_revision: string;
		acl_revision: string;
		content_digest: string;
	}>(
		`SELECT s.source_id, s.project_id, s.current_revision::text AS current_revision, s.acl_revision::text AS acl_revision,
		   r.content_digest
		 FROM sources s JOIN source_revisions r ON r.source_id = s.source_id AND r.revision = s.current_revision
		 WHERE s.source_id = ANY($4::text[]) AND s.tenant_id = $1 AND NOT s.deleted AND ($2 = '' OR s.project_id = $2)
		   AND EXISTS (SELECT 1 FROM source_acl a WHERE a.source_id = s.source_id AND a.acl_revision = s.acl_revision
		               AND a.principal_type || ':' || a.principal_id = ANY($3::text[]))`,
		[tenantId, projectId, principals, sourceIds],
	);
	for (const x of r.rows)
		out.set(x.source_id, {
			sourceId: x.source_id,
			projectId: x.project_id,
			currentRevision: Number(x.current_revision),
			aclRevision: Number(x.acl_revision),
			contentDigest: x.content_digest,
		});
	return out;
}

interface SnapshotRow {
	snapshot_id: string;
	tenant_id: string;
	project_id: string;
	generation: string;
	content_digest: string;
	request_digest: string;
	created_at: Date;
}

async function sourcesOf(c: Queryable, snapshotId: string): Promise<SnapshotSource[]> {
	const r = await c.query<{ source_id: string; source_revision: string; content_digest: string }>(
		`SELECT x.source_id, x.source_revision::text AS source_revision, r.content_digest
		 FROM snapshot_sources x JOIN source_revisions r ON r.source_id = x.source_id AND r.revision = x.source_revision
		 WHERE x.snapshot_id = $1 ORDER BY x.source_id`,
		[snapshotId],
	);
	return r.rows.map((x) => ({
		sourceId: x.source_id,
		sourceRevision: Number(x.source_revision),
		contentDigest: x.content_digest,
	}));
}

async function fromRow(c: Queryable, x: SnapshotRow): Promise<Snapshot> {
	return {
		snapshotId: x.snapshot_id,
		tenantId: x.tenant_id,
		projectId: x.project_id,
		generation: Number(x.generation),
		sources: await sourcesOf(c, x.snapshot_id),
		contentDigest: x.content_digest,
		requestDigest: x.request_digest,
		createdAt: x.created_at,
	};
}

const snapshotSelect = `SELECT snapshot_id, tenant_id, project_id, generation::text AS generation, content_digest, request_digest,
	created_at FROM snapshots`;

export async function getSnapshot(c: Queryable, snapshotId: string): Promise<Snapshot | undefined> {
	const r = await c.query<SnapshotRow>(`${snapshotSelect} WHERE snapshot_id = $1`, [snapshotId]);
	return r.rows[0] ? fromRow(c, r.rows[0]) : undefined;
}

export async function getSnapshotByCommand(
	c: Queryable,
	tenantId: string,
	commandId: string,
): Promise<Snapshot | undefined> {
	const r = await c.query<SnapshotRow>(`${snapshotSelect} WHERE tenant_id = $1 AND command_id = $2`, [
		tenantId,
		commandId,
	]);
	return r.rows[0] ? fromRow(c, r.rows[0]) : undefined;
}

export async function insertSnapshot(c: PoolClient, s: Snapshot, commandId: string): Promise<void> {
	await c.query(
		`INSERT INTO snapshots (snapshot_id, tenant_id, project_id, generation, content_digest, command_id, request_digest)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		[s.snapshotId, s.tenantId, s.projectId, s.generation, s.contentDigest, commandId, s.requestDigest],
	);
	await c.query(
		`INSERT INTO snapshot_sources (snapshot_id, source_id, source_revision)
		 SELECT $1, x.source_id, x.source_revision FROM unnest($2::text[], $3::bigint[]) AS x(source_id, source_revision)`,
		[s.snapshotId, s.sources.map((x) => x.sourceId), s.sources.map((x) => x.sourceRevision)],
	);
}
