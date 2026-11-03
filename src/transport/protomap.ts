// Knowledge's wire mapping: domain records to their anvilkit.knowledge.v1
// messages, and the pure decoders of request fields (no caller context).

import {
	AccessEntry,
	BackgroundTask,
	FactDecision,
	FactOrigin,
	FactState,
	IndexState,
	IngestState,
	MemoryFact,
	Snapshot,
	Source,
	SourceKind,
	TaskState,
} from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import type { IndexAnswer } from "../application/indexer.js";
import type { ProjectionAnswer } from "../application/projection.js";
import type { Fact } from "../domain/memory.js";
import type { Snapshot as DomainSnapshot } from "../domain/retrieval.js";
import * as domain from "../domain/source.js";
import { type Task, TaskError } from "../domain/task.js";

const stateOf: Record<Task["state"], TaskState> = {
	pending: TaskState.TASK_STATE_PENDING,
	leased: TaskState.TASK_STATE_LEASED,
	result_submitted: TaskState.TASK_STATE_RESULT_SUBMITTED,
	accepted: TaskState.TASK_STATE_ACCEPTED,
	retry_scheduled: TaskState.TASK_STATE_RETRY_SCHEDULED,
	dead: TaskState.TASK_STATE_DEAD,
	stale: TaskState.TASK_STATE_STALE,
	canceled: TaskState.TASK_STATE_CANCELED,
};

export function toProto(t: Task): BackgroundTask {
	return BackgroundTask.fromPartial({
		taskId: t.taskId,
		generation: String(t.generation),
		taskKind: t.kind,
		inputDigest: t.inputDigest,
		state: stateOf[t.state],
		workerId: t.workerId || undefined,
		leaseUntil: t.state === "leased" && t.leaseUntil ? t.leaseUntil : undefined,
		attemptCount: String(t.attemptCount),
	});
}

export const kindOf: Record<number, domain.SourceKind> = {
	[SourceKind.SOURCE_KIND_DOCUMENT]: "document",
	[SourceKind.SOURCE_KIND_URL]: "url",
	[SourceKind.SOURCE_KIND_REPOSITORY]: "repository",
	[SourceKind.SOURCE_KIND_BRAND]: "brand",
};
const kindTo: Record<domain.SourceKind, SourceKind> = {
	document: SourceKind.SOURCE_KIND_DOCUMENT,
	url: SourceKind.SOURCE_KIND_URL,
	repository: SourceKind.SOURCE_KIND_REPOSITORY,
	brand: SourceKind.SOURCE_KIND_BRAND,
};
const ingestTo: Record<domain.IngestState, IngestState> = {
	pending: IngestState.INGEST_STATE_PENDING,
	parsing: IngestState.INGEST_STATE_PARSING,
	indexing: IngestState.INGEST_STATE_INDEXING,
	indexed: IngestState.INGEST_STATE_INDEXED,
	failed: IngestState.INGEST_STATE_FAILED,
	stale: IngestState.INGEST_STATE_STALE,
};

/** The public Source: identities, revisions, digest, ACL and ingest state; never the storage key. */
export function sourceToProto(s: domain.Source): Source {
	return Source.fromPartial({
		sourceId: s.sourceId,
		tenantId: s.tenantId,
		projectId: s.projectId,
		kind: kindTo[s.kind],
		locator: s.locator,
		currentRevision: String(s.currentRevision),
		contentDigest: s.contentDigest,
		aclRevision: String(s.aclRevision),
		access: s.access.map((e) =>
			AccessEntry.fromPartial({
				principalType: e.principalType,
				principalId: e.principalId,
				permission: e.permission ?? "read",
			}),
		),
		ingest: ingestTo[s.ingest],
		deleted: s.deleted,
		createdAt: s.createdAt,
		updatedAt: s.updatedAt,
	});
}

/** The public Snapshot: identities, the generation, exact revisions (<source>@<revision>) and the frozen digest. */
export function snapshotToProto(s: DomainSnapshot): Snapshot {
	return Snapshot.fromPartial({
		snapshotId: s.snapshotId,
		tenantId: s.tenantId,
		projectId: s.projectId,
		indexGeneration: String(s.generation),
		sourceRevisions: s.sources.map((x) => `${x.sourceId}@${x.sourceRevision}`),
		contentDigest: s.contentDigest,
		createdAt: s.createdAt,
	});
}

const factStateTo: Record<Fact["state"], FactState> = {
	proposed: FactState.FACT_STATE_PROPOSED,
	confirmed: FactState.FACT_STATE_CONFIRMED,
	rejected: FactState.FACT_STATE_REJECTED,
	revoked: FactState.FACT_STATE_REVOKED,
	expired: FactState.FACT_STATE_EXPIRED,
};
export const factStateOf: Record<number, Fact["state"]> = {
	[FactState.FACT_STATE_PROPOSED]: "proposed",
	[FactState.FACT_STATE_CONFIRMED]: "confirmed",
	[FactState.FACT_STATE_REJECTED]: "rejected",
	[FactState.FACT_STATE_REVOKED]: "revoked",
	[FactState.FACT_STATE_EXPIRED]: "expired",
};
export const originOf: Record<number, Fact["origin"]> = {
	[FactOrigin.FACT_ORIGIN_USER]: "user",
	[FactOrigin.FACT_ORIGIN_MODEL]: "model",
	[FactOrigin.FACT_ORIGIN_WORKER]: "worker",
};
const originTo: Record<Fact["origin"], FactOrigin> = {
	user: FactOrigin.FACT_ORIGIN_USER,
	model: FactOrigin.FACT_ORIGIN_MODEL,
	worker: FactOrigin.FACT_ORIGIN_WORKER,
};
export const decisionOf: Record<number, "confirm" | "reject" | "revoke"> = {
	[FactDecision.FACT_DECISION_CONFIRM]: "confirm",
	[FactDecision.FACT_DECISION_REJECT]: "reject",
	[FactDecision.FACT_DECISION_REVOKE]: "revoke",
};

/** The public MemoryFact: identities, state, revision, provenance and, unless deleted, the content. */
export function factToProto(f: Fact): MemoryFact {
	return MemoryFact.fromPartial({
		factId: f.factId,
		tenantId: f.tenantId,
		subjectType: f.subjectType,
		subjectId: f.subjectId,
		scopeId: f.scopeId,
		content: f.deleted ? "" : f.content,
		contentDigest: f.contentDigest,
		state: factStateTo[f.state],
		revision: String(f.revision),
		proposer: f.proposer,
		confirmer: f.confirmer || undefined,
		sourceRefs: f.sourceRefs,
		expiresAt: f.expiresAt ?? undefined,
		createdAt: f.createdAt,
		updatedAt: f.updatedAt,
		origin: originTo[f.origin],
		deleted: f.deleted,
	});
}

export function commandOf(
	c: { tenantId: string; commandId: string; actorId: string; requestDigest: string } | undefined,
) {
	if (!c) throw new domain.SourceError("INVALID_ARGUMENT", "command is required");
	return { tenantId: c.tenantId, commandId: c.commandId, actorId: c.actorId, requestDigest: c.requestDigest };
}

export function accessOf(entries: AccessEntry[]): domain.AccessEntry[] {
	return entries.map((e) => ({
		principalType: e.principalType as domain.PrincipalType,
		principalId: e.principalId,
		...(e.permission === "manage" ? { permission: "manage" as const } : {}),
	}));
}

export function sequenceOf(v: string, what: string): number {
	if (!/^(0|[1-9][0-9]{0,15})$/.test(v)) throw new domain.SourceError("INVALID_ARGUMENT", what);
	return Number(v);
}

export function generationOf(s: string): number {
	if (!/^[1-9][0-9]{0,18}$/.test(s)) throw new TaskError("INVALID_ARGUMENT", `generation ${s}`);
	return Number(s);
}

/** The fields of an index-shaped advance answer (AdvanceIndex, AdvanceProjection): its IndexState and what that state carries. */
export function indexAnswerOf(a: IndexAnswer | ProjectionAnswer): {
	state: IndexState;
	resultRef: string;
	resultDigest: string;
	failureCode: string | undefined;
	retryAfterMs: number;
} {
	return {
		state:
			a.state === "running"
				? IndexState.INDEX_STATE_RUNNING
				: a.state === "materialized"
					? IndexState.INDEX_STATE_MATERIALIZED
					: IndexState.INDEX_STATE_FAILED,
		resultRef: a.state === "materialized" ? a.resultRef : "",
		resultDigest: a.state === "materialized" ? a.resultDigest : "",
		failureCode: a.state === "failed" ? a.failureCode : undefined,
		retryAfterMs: a.state === "running" ? a.retryAfterMs : 0,
	};
}
