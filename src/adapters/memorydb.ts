// The reviewed statements of MemoryFact, its append-only decisions and its
// projection ledger (migrations 00001/00005). Visibility and recallability
// are the domain rules of domain/memory.ts evaluated in SQL so a listing or
// a recall's allowed set is computed by the authority, never by a cache,
// the Store or Qdrant. Nothing here reads the Store or Qdrant.
import type pg from "pg";
import type { Fact, FactOrigin, FactState, ProjectionAction, SubjectType } from "../domain/memory.js";
import type { PoolClient } from "./postgres.js";

type Queryable = pg.Pool | PoolClient;

interface FactRow {
	fact_id: string;
	tenant_id: string;
	subject_type: string;
	subject_id: string;
	scope_id: string;
	content: string;
	content_digest: string;
	state: string;
	revision: string;
	proposer: string;
	confirmer: string | null;
	origin: string;
	source_refs: string[];
	expires_at: Date | null;
	deleted: boolean;
	deleted_at: Date | null;
	purged_at: Date | null;
	created_at: Date;
	updated_at: Date;
}

const factColumns = `fact_id, tenant_id, subject_type, subject_id, scope_id, content, content_digest, state,
	revision::text AS revision, proposer, confirmer, origin, source_refs, expires_at, deleted, deleted_at, purged_at,
	created_at, updated_at`;

function factOf(r: FactRow): Fact {
	return {
		factId: r.fact_id,
		tenantId: r.tenant_id,
		subjectType: r.subject_type as SubjectType,
		subjectId: r.subject_id,
		scopeId: r.scope_id,
		content: r.content,
		contentDigest: r.content_digest,
		state: r.state as FactState,
		revision: Number(r.revision),
		proposer: r.proposer,
		confirmer: r.confirmer ?? "",
		origin: r.origin as FactOrigin,
		sourceRefs: r.source_refs,
		expiresAt: r.expires_at,
		deleted: r.deleted,
		deletedAt: r.deleted_at,
		purgedAt: r.purged_at,
		createdAt: r.created_at,
		updatedAt: r.updated_at,
	};
}

export async function getFact(c: Queryable, factId: string, forUpdate = false): Promise<Fact | undefined> {
	const r = await c.query<FactRow>(
		`SELECT ${factColumns} FROM memory_facts WHERE fact_id = $1${forUpdate ? " FOR UPDATE" : ""}`,
		[factId],
	);
	return r.rows[0] ? factOf(r.rows[0]) : undefined;
}

export async function getFacts(c: Queryable, factIds: string[]): Promise<Map<string, Fact>> {
	const out = new Map<string, Fact>();
	if (factIds.length === 0) return out;
	const r = await c.query<FactRow>(`SELECT ${factColumns} FROM memory_facts WHERE fact_id = ANY($1::text[])`, [
		factIds,
	]);
	for (const x of r.rows) out.set(x.fact_id, factOf(x));
	return out;
}

export async function insertFact(c: PoolClient, f: Fact, commandId: string, requestDigest: string): Promise<void> {
	await c.query(
		`INSERT INTO memory_facts (fact_id, tenant_id, subject_type, subject_id, scope_id, content, content_digest, state, revision,
		   proposer, origin, source_refs, expires_at, command_id, request_digest)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		[
			f.factId,
			f.tenantId,
			f.subjectType,
			f.subjectId,
			f.scopeId,
			f.content,
			f.contentDigest,
			f.state,
			f.revision,
			f.proposer,
			f.origin,
			f.sourceRefs,
			f.expiresAt,
			commandId,
			requestDigest,
		],
	);
}

/** The CAS update of a decision: binds the revision the transaction read. */
export async function updateFact(c: PoolClient, f: Fact, readRevision: number): Promise<number> {
	const r = await c.query(
		`UPDATE memory_facts SET state = $2, revision = $3, confirmer = $4, expires_at = $5, content = $6, deleted = $7,
		   deleted_at = $8, purged_at = $9, updated_at = now()
		 WHERE fact_id = $1 AND revision = $10`,
		[
			f.factId,
			f.state,
			f.revision,
			f.confirmer || null,
			f.expiresAt,
			f.content,
			f.deleted,
			f.deletedAt,
			f.purgedAt,
			readRevision,
		],
	);
	return r.rowCount ?? 0;
}

export interface DecisionRow {
	decisionId: string;
	factId: string;
	tenantId: string;
	fromRevision: number;
	toRevision: number;
	decision: "propose" | "confirm" | "reject" | "revoke" | "expire" | "delete";
	decider: string;
	authority: "user" | "model" | "worker" | "policy";
	policyRevision: string;
	reasonCode: string;
	sourceRefs: string[];
	expiresAt: Date | null;
	commandId: string;
	requestDigest: string;
}

export async function insertDecision(c: PoolClient, d: DecisionRow): Promise<void> {
	await c.query(
		`INSERT INTO memory_decisions (decision_id, fact_id, tenant_id, from_revision, to_revision, decision, decider, authority,
		   policy_revision, reason_code, source_refs, expires_at, command_id, request_digest)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		[
			d.decisionId,
			d.factId,
			d.tenantId,
			d.fromRevision,
			d.toRevision,
			d.decision,
			d.decider,
			d.authority,
			d.policyRevision || null,
			d.reasonCode || null,
			d.sourceRefs,
			d.expiresAt,
			d.commandId,
			d.requestDigest,
		],
	);
}

/** The decision a (tenant, command) already took, if any (command idempotency). */
export async function decisionByCommand(
	c: Queryable,
	tenantId: string,
	commandId: string,
): Promise<{ factId: string; decision: DecisionRow["decision"]; requestDigest: string; decider: string } | undefined> {
	const r = await c.query<{
		fact_id: string;
		decision: DecisionRow["decision"];
		request_digest: string;
		decider: string;
	}>(
		"SELECT fact_id, decision, request_digest, decider FROM memory_decisions WHERE tenant_id = $1 AND command_id = $2",
		[tenantId, commandId],
	);
	const x = r.rows[0];
	return x
		? { factId: x.fact_id, decision: x.decision, requestDigest: x.request_digest, decider: x.decider }
		: undefined;
}

/** The decision history of a fact, oldest first (audit and tests). */
export async function decisionsOf(c: Queryable, factId: string): Promise<DecisionRow[]> {
	const r = await c.query<{
		decision_id: string;
		fact_id: string;
		tenant_id: string;
		from_revision: string;
		to_revision: string;
		decision: DecisionRow["decision"];
		decider: string;
		authority: DecisionRow["authority"];
		policy_revision: string | null;
		reason_code: string | null;
		source_refs: string[];
		expires_at: Date | null;
		command_id: string;
		request_digest: string;
	}>(
		`SELECT decision_id, fact_id, tenant_id, from_revision::text AS from_revision, to_revision::text AS to_revision, decision,
		   decider, authority, policy_revision, reason_code, source_refs, expires_at, command_id, request_digest
		 FROM memory_decisions WHERE fact_id = $1 ORDER BY to_revision`,
		[factId],
	);
	return r.rows.map((x) => ({
		decisionId: x.decision_id,
		factId: x.fact_id,
		tenantId: x.tenant_id,
		fromRevision: Number(x.from_revision),
		toRevision: Number(x.to_revision),
		decision: x.decision,
		decider: x.decider,
		authority: x.authority,
		policyRevision: x.policy_revision ?? "",
		reasonCode: x.reason_code ?? "",
		sourceRefs: x.source_refs,
		expiresAt: x.expires_at,
		commandId: x.command_id,
		requestDigest: x.request_digest,
	}));
}

/** Whether an identity ever proposed on behalf of a model or a worker in the tenant. */
export async function speaksForModel(c: Queryable, tenantId: string, actorId: string): Promise<boolean> {
	const r = await c.query(
		"SELECT 1 FROM memory_facts WHERE tenant_id = $1 AND proposer = $2 AND origin IN ('model', 'worker') LIMIT 1",
		[tenantId, actorId],
	);
	return (r.rowCount ?? 0) > 0;
}

/** Another live confirmed fact with the same content, subject and scope (the conflict rule). */
export async function confirmedDuplicate(c: Queryable, f: Fact): Promise<boolean> {
	const r = await c.query(
		`SELECT 1 FROM memory_facts WHERE tenant_id = $1 AND scope_id = $2 AND subject_type = $3 AND subject_id = $4
		   AND content_digest = $5 AND state = 'confirmed' AND NOT deleted AND fact_id <> $6 LIMIT 1`,
		[f.tenantId, f.scopeId, f.subjectType, f.subjectId, f.contentDigest, f.factId],
	);
	return (r.rowCount ?? 0) > 0;
}

/** The visibility rule of domain/memory.ts over the reader's scope. */
const visibleWhere = `tenant_id = $1 AND NOT deleted AND (scope_id = '' OR scope_id = $2)
	AND (subject_type <> 'actor' OR subject_id = $3)`;

export async function listVisible(
	c: Queryable,
	scope: { tenantId: string; projectId: string; actorId: string },
	filter: { subjectType: string; subjectId: string; state: string },
	after: string,
	limit: number,
): Promise<Fact[]> {
	const r = await c.query<FactRow>(
		`SELECT ${factColumns} FROM memory_facts WHERE ${visibleWhere}
		   AND ($4 = '' OR subject_type = $4) AND ($5 = '' OR subject_id = $5) AND ($6 = '' OR state = $6) AND fact_id > $7
		 ORDER BY fact_id LIMIT $8`,
		[scope.tenantId, scope.projectId, scope.actorId, filter.subjectType, filter.subjectId, filter.state, after, limit],
	);
	return r.rows.map(factOf);
}

export interface AllowedFact {
	factId: string;
	revision: number;
	sourceRefs: string[];
}

/**
 * The facts a reader may recall now: visible, confirmed, not deleted and
 * not expired at `now`. Provenance readability is checked by the caller
 * against the Source Registry. At most `limit` rows are returned; the
 * caller refuses a set that reaches the bound instead of truncating it.
 */
export async function recallableFor(
	c: Queryable,
	scope: { tenantId: string; projectId: string; actorId: string },
	now: Date,
	limit: number,
	factIds?: string[],
): Promise<AllowedFact[]> {
	const r = await c.query<{ fact_id: string; revision: string; source_refs: string[] }>(
		`SELECT fact_id, revision::text AS revision, source_refs FROM memory_facts
		 WHERE ${visibleWhere} AND state = 'confirmed' AND (expires_at IS NULL OR expires_at > $4)
		   AND ($6::text[] IS NULL OR fact_id = ANY($6::text[]))
		 ORDER BY fact_id LIMIT $5`,
		[scope.tenantId, scope.projectId, scope.actorId, now, limit, factIds ?? null],
	);
	return r.rows.map((x) => ({ factId: x.fact_id, revision: Number(x.revision), sourceRefs: x.source_refs }));
}

/** Confirmed facts whose expiry passed (the reviewed expiry rule's work list). */
export async function expiredFacts(c: Queryable, now: Date, limit: number): Promise<string[]> {
	const r = await c.query<{ fact_id: string }>(
		`SELECT fact_id FROM memory_facts WHERE state = 'confirmed' AND NOT deleted AND expires_at IS NOT NULL AND expires_at <= $1
		 ORDER BY expires_at, fact_id LIMIT $2`,
		[now, limit],
	);
	return r.rows.map((x) => x.fact_id);
}

// ---------------------------------------------------------------------------
// Projection ledger
// ---------------------------------------------------------------------------

export type ProjectionState = "pending" | "running" | "materialized" | "accepted" | "failed" | "stale";

export interface Projection {
	factId: string;
	target: number;
	factRevision: number;
	action: ProjectionAction;
	epoch: number;
	taskId: string;
	state: ProjectionState;
	manifestDigest: string;
	failureCode: string;
	updatedAt: Date;
}

interface ProjectionRow {
	fact_id: string;
	target: string;
	fact_revision: string;
	action: ProjectionAction;
	epoch: number;
	task_id: string;
	state: ProjectionState;
	manifest_digest: string | null;
	failure_code: string | null;
	updated_at: Date;
}

const projectionColumns = `fact_id, target::text AS target, fact_revision::text AS fact_revision, action, epoch, task_id, state,
	manifest_digest, failure_code, updated_at`;

function projectionOf(x: ProjectionRow): Projection {
	return {
		factId: x.fact_id,
		target: Number(x.target),
		factRevision: Number(x.fact_revision),
		action: x.action,
		epoch: x.epoch,
		taskId: x.task_id,
		state: x.state,
		manifestDigest: x.manifest_digest ?? "",
		failureCode: x.failure_code ?? "",
		updatedAt: x.updated_at,
	};
}

export async function projectionsOf(c: Queryable, factId: string, forUpdate = false): Promise<Projection[]> {
	const r = await c.query<ProjectionRow>(
		`SELECT ${projectionColumns} FROM memory_projections WHERE fact_id = $1 ORDER BY target${forUpdate ? " FOR UPDATE" : ""}`,
		[factId],
	);
	return r.rows.map(projectionOf);
}

export async function projectionByTask(
	c: Queryable,
	taskId: string,
	forUpdate = false,
): Promise<Projection | undefined> {
	const r = await c.query<ProjectionRow>(
		`SELECT ${projectionColumns} FROM memory_projections WHERE task_id = $1${forUpdate ? " FOR UPDATE" : ""}`,
		[taskId],
	);
	return r.rows[0] ? projectionOf(r.rows[0]) : undefined;
}

/** Points the (fact, target) row at a new revision/action/epoch, pending again. */
export async function upsertProjection(
	c: PoolClient,
	p: Pick<Projection, "factId" | "target" | "factRevision" | "action" | "epoch" | "taskId">,
): Promise<void> {
	await c.query(
		`INSERT INTO memory_projections (fact_id, target, fact_revision, action, epoch, task_id, state)
		 VALUES ($1, $2, $3, $4, $5, $6, 'pending')
		 ON CONFLICT (fact_id, target) DO UPDATE SET fact_revision = $3, action = $4, epoch = $5, state = 'pending',
		   manifest_digest = NULL, failure_code = NULL, updated_at = now()`,
		[p.factId, p.target, p.factRevision, p.action, p.epoch, p.taskId],
	);
}

/** Updates the row a task carries, only while it still carries that revision and epoch. */
export async function updateProjection(
	c: Queryable,
	taskId: string,
	at: { factRevision: number; epoch: number },
	patch: { state: ProjectionState; manifestDigest?: string; failureCode?: string },
): Promise<number> {
	const r = await c.query(
		`UPDATE memory_projections SET state = $4, manifest_digest = COALESCE($5, manifest_digest), failure_code = $6, updated_at = now()
		 WHERE task_id = $1 AND fact_revision = $2 AND epoch = $3`,
		[taskId, at.factRevision, at.epoch, patch.state, patch.manifestDigest ?? null, patch.failureCode ?? null],
	);
	return r.rowCount ?? 0;
}

/**
 * Facts a target lacks at their current revision: for the Store every
 * confirmed fact and every fact with a ledger row elsewhere; for a vector
 * generation every recallable-state (confirmed, undeleted) fact. Used by the
 * backfill of new generations and by the rebuild.
 */
export async function missingFor(c: Queryable, target: number, limit: number): Promise<Fact[]> {
	const r = await c.query<FactRow>(
		`SELECT ${factColumns} FROM memory_facts f
		 WHERE ((f.state = 'confirmed' AND NOT f.deleted)
		        OR ($1 = 0 AND EXISTS (SELECT 1 FROM memory_projections o WHERE o.fact_id = f.fact_id)))
		   AND NOT EXISTS (SELECT 1 FROM memory_projections p WHERE p.fact_id = f.fact_id AND p.target = $1 AND p.fact_revision = f.revision)
		 ORDER BY f.fact_id LIMIT $2`,
		[target, limit],
	);
	return r.rows.map(factOf);
}

/** Failed rows of the facts' current revisions whose retry delay passed. */
export async function failedProjections(
	c: Queryable,
	delayMs: number,
	maxEpoch: number,
	limit: number,
): Promise<Projection[]> {
	const r = await c.query<ProjectionRow>(
		`SELECT p.fact_id, p.target::text AS target, p.fact_revision::text AS fact_revision, p.action, p.epoch, p.task_id, p.state,
		   p.manifest_digest, p.failure_code, p.updated_at
		 FROM memory_projections p JOIN memory_facts f ON f.fact_id = p.fact_id AND f.revision = p.fact_revision
		 WHERE p.state IN ('failed', 'stale') AND p.updated_at <= now() - make_interval(secs => $1::float8 / 1000) AND p.epoch < $2
		 ORDER BY p.updated_at LIMIT $3`,
		[delayMs, maxEpoch, limit],
	);
	return r.rows.map(projectionOf);
}

/**
 * What a vector generation needs before it qualifies: every confirmed,
 * undeleted fact has an accepted apply row there at its current revision,
 * and no row of the generation is open.
 */
export async function memoryProgress(
	c: Queryable,
	generation: number,
): Promise<{ live: number; accepted: number; open: number }> {
	const r = await c.query<{ live: string; accepted: string; open: string }>(
		`SELECT (SELECT count(*) FROM memory_facts WHERE state = 'confirmed' AND NOT deleted)::text AS live,
		   (SELECT count(*) FROM memory_projections p JOIN memory_facts f ON f.fact_id = p.fact_id AND f.revision = p.fact_revision
		     WHERE p.target = $1 AND p.state = 'accepted' AND p.action = 'apply' AND f.state = 'confirmed' AND NOT f.deleted)::text AS accepted,
		   (SELECT count(*) FROM memory_projections WHERE target = $1 AND state IN ('pending', 'running', 'materialized'))::text AS open`,
		[generation],
	);
	const x = r.rows[0];
	return { live: Number(x?.live ?? 0), accepted: Number(x?.accepted ?? 0), open: Number(x?.open ?? 0) };
}

/** A deleted fact is purged once every row applied its deletion revision's removal. */
export async function markPurgedIfComplete(c: PoolClient, factId: string): Promise<boolean> {
	const r = await c.query(
		`UPDATE memory_facts f SET purged_at = now()
		 WHERE f.fact_id = $1 AND f.deleted AND f.purged_at IS NULL
		   AND NOT EXISTS (SELECT 1 FROM memory_projections p WHERE p.fact_id = f.fact_id
		                   AND NOT (p.fact_revision = f.revision AND p.action = 'remove' AND p.state = 'accepted'))`,
		[factId],
	);
	return (r.rowCount ?? 0) === 1;
}
