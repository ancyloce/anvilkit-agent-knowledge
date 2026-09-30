// Knowledge's grpc-js transport (A03): anvilkit.knowledge.v1
// BackgroundTaskService (P14), SourceService and IngestService (P15). Every
// decoded request passes
// the contract's explicit TypeScript validation (protovalidate over the
// generated descriptors) before its handler; domain errors map to the
// public codes of contracts.md §4. The listener is plaintext
// (DEVELOPMENT_ONLY; workload mTLS is ENV-03), like the other new services.
import {
	AccessEntry,
	AdvanceIndexRequest,
	type AdvanceIndexResponse,
	AdvanceParseRequest,
	type AdvanceParseResponse,
	AdvanceProjectionRequest,
	type AdvanceProjectionResponse,
	BackgroundTask,
	type BackgroundTaskServiceServer,
	BackgroundTaskServiceService,
	Citation,
	ClaimTaskRequest,
	type ClaimTaskResponse,
	ContextItem,
	CreateSnapshotRequest,
	type CreateSnapshotResponse,
	DecideFactRequest,
	type DecideFactResponse,
	DeleteFactRequest,
	type DeleteFactResponse,
	DeleteSourceRequest,
	type DeleteSourceResponse,
	FactDecision,
	FactOrigin,
	FactState,
	GetFactRequest,
	type GetFactResponse,
	GetSnapshotRequest,
	type GetSnapshotResponse,
	GetSourceRequest,
	type GetSourceResponse,
	GetTaskRequest,
	type GetTaskResponse,
	HeartbeatTaskRequest,
	type HeartbeatTaskResponse,
	IndexState,
	type IngestServiceServer,
	IngestServiceService,
	IngestState,
	ListFactsRequest,
	type ListFactsResponse,
	ListSourcesRequest,
	type ListSourcesResponse,
	MemoryFact,
	type MemoryServiceServer,
	MemoryServiceService,
	ParseState,
	ProposeFactRequest,
	type ProposeFactResponse,
	RecallFactsRequest,
	type RecallFactsResponse,
	RegisterSourceRequest,
	type RegisterSourceResponse,
	type RetrievalServiceServer,
	RetrievalServiceService,
	SearchRequest,
	type SearchResponse,
	Snapshot,
	type SnapshotServiceServer,
	SnapshotServiceService,
	Source,
	SourceKind,
	type SourceServiceServer,
	SourceServiceService,
	SubmitTaskResultRequest,
	type SubmitTaskResultResponse,
	TaskState,
	UpdateSourceAccessRequest,
	type UpdateSourceAccessResponse,
} from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { validateJson } from "@anvilkit/generated-clients/validation/rpc";
import {
	Server,
	ServerCredentials,
	type ServerUnaryCall,
	type ServiceError,
	type sendUnaryData,
	status,
} from "@grpc/grpc-js";
import { HealthImplementation } from "grpc-health-check";
import type { Indexer } from "../application/indexer.js";
import type { Ingest } from "../application/ingest.js";
import type { Memory } from "../application/memory.js";
import type { MemoryProjector } from "../application/projection.js";
import type { Recall } from "../application/recall.js";
import type { Retrieval } from "../application/retrieval.js";
import type { Snapshots } from "../application/snapshots.js";
import type { Sources } from "../application/sources.js";
import type { Tasks } from "../application/tasks.js";
import { IndexError } from "../domain/index.js";
import { type Fact, MemoryError } from "../domain/memory.js";
import { ParseError } from "../domain/parse.js";
import { type Snapshot as DomainSnapshot, RetrievalError } from "../domain/retrieval.js";
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

const kindOf: Record<number, domain.SourceKind> = {
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
			AccessEntry.fromPartial({ principalType: e.principalType, principalId: e.principalId }),
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
const factStateOf: Record<number, Fact["state"]> = {
	[FactState.FACT_STATE_PROPOSED]: "proposed",
	[FactState.FACT_STATE_CONFIRMED]: "confirmed",
	[FactState.FACT_STATE_REJECTED]: "rejected",
	[FactState.FACT_STATE_REVOKED]: "revoked",
	[FactState.FACT_STATE_EXPIRED]: "expired",
};
const originOf: Record<number, Fact["origin"]> = {
	[FactOrigin.FACT_ORIGIN_USER]: "user",
	[FactOrigin.FACT_ORIGIN_MODEL]: "model",
	[FactOrigin.FACT_ORIGIN_WORKER]: "worker",
};
const originTo: Record<Fact["origin"], FactOrigin> = {
	user: FactOrigin.FACT_ORIGIN_USER,
	model: FactOrigin.FACT_ORIGIN_MODEL,
	worker: FactOrigin.FACT_ORIGIN_WORKER,
};
const decisionOf: Record<number, "confirm" | "reject" | "revoke"> = {
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

function commandOf(c: { tenantId: string; commandId: string; actorId: string; requestDigest: string } | undefined) {
	if (!c) throw new domain.SourceError("INVALID_ARGUMENT", "command is required");
	return { tenantId: c.tenantId, commandId: c.commandId, actorId: c.actorId, requestDigest: c.requestDigest };
}

function scopeOf(s: { tenantId: string; projectId: string; actorId: string } | undefined): domain.Scope {
	if (!s) throw new domain.SourceError("INVALID_ARGUMENT", "scope is required");
	return { tenantId: s.tenantId, projectId: s.projectId, actorId: s.actorId };
}

function accessOf(entries: AccessEntry[]): domain.AccessEntry[] {
	return entries.map((e) => ({ principalType: e.principalType as domain.PrincipalType, principalId: e.principalId }));
}

function sequenceOf(v: string, what: string): number {
	if (!/^(0|[1-9][0-9]{0,15})$/.test(v)) throw new domain.SourceError("INVALID_ARGUMENT", what);
	return Number(v);
}

function toStatus(err: unknown): ServiceError {
	const e = (code: status, message: string) =>
		Object.assign(new Error(message), { code, details: message, metadata: undefined }) as unknown as ServiceError;
	if (err instanceof ParseError) {
		switch (err.code) {
			case "NOT_FOUND":
				return e(status.NOT_FOUND, "NOT_FOUND");
			case "STALE_EXECUTION":
				return e(status.FAILED_PRECONDITION, err.message);
			case "INVALID_ARGUMENT":
				return e(status.INVALID_ARGUMENT, err.message);
			default:
				return e(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
		}
	}
	if (err instanceof IndexError) {
		switch (err.code) {
			case "NOT_FOUND":
				return e(status.NOT_FOUND, "NOT_FOUND");
			case "STALE_EXECUTION":
				return e(status.FAILED_PRECONDITION, err.message);
			case "INVALID_ARGUMENT":
				return e(status.INVALID_ARGUMENT, err.message);
			default:
				return e(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
		}
	}
	if (err instanceof RetrievalError) {
		switch (err.code) {
			case "NOT_FOUND":
				return e(status.NOT_FOUND, "NOT_FOUND");
			case "FORBIDDEN":
				return e(status.PERMISSION_DENIED, err.message);
			case "COMMAND_CONFLICT":
				return e(status.ALREADY_EXISTS, err.message);
			case "NOT_INDEXED":
			case "PROFILE_UNQUALIFIED":
				return e(status.FAILED_PRECONDITION, err.message);
			case "SCOPE_TOO_LARGE":
				return e(status.RESOURCE_EXHAUSTED, err.message);
			case "DEADLINE_EXCEEDED":
				return e(status.DEADLINE_EXCEEDED, "DEADLINE_EXCEEDED");
			case "UNAVAILABLE":
				return e(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
			default:
				return e(status.INVALID_ARGUMENT, err.message);
		}
	}
	if (err instanceof MemoryError) {
		switch (err.code) {
			case "NOT_FOUND":
				return e(status.NOT_FOUND, "NOT_FOUND");
			case "FORBIDDEN":
				return e(status.PERMISSION_DENIED, err.message);
			case "COMMAND_CONFLICT":
				return e(status.ALREADY_EXISTS, err.message);
			case "REVISION_MISMATCH":
			case "INVALID_TRANSITION":
			case "PROVENANCE_STALE":
			case "FACT_CONFLICT":
			case "EXPIRED":
			case "PROFILE_UNQUALIFIED":
			case "STALE_EXECUTION":
				return e(status.FAILED_PRECONDITION, err.message);
			case "SCOPE_TOO_LARGE":
				return e(status.RESOURCE_EXHAUSTED, err.message);
			case "DEADLINE_EXCEEDED":
				return e(status.DEADLINE_EXCEEDED, "DEADLINE_EXCEEDED");
			case "UNAVAILABLE":
				return e(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
			default:
				return e(status.INVALID_ARGUMENT, err.message);
		}
	}
	if (err instanceof domain.SourceError) {
		switch (err.code) {
			case "NOT_FOUND":
				return e(status.NOT_FOUND, "NOT_FOUND");
			case "FORBIDDEN":
				return e(status.PERMISSION_DENIED, err.message);
			case "COMMAND_CONFLICT":
				return e(status.ALREADY_EXISTS, err.message);
			case "REVISION_MISMATCH":
				return e(status.FAILED_PRECONDITION, err.message);
			default:
				return e(status.INVALID_ARGUMENT, err.message);
		}
	}
	if (err instanceof TaskError) {
		switch (err.code) {
			case "NOT_FOUND":
				return e(status.NOT_FOUND, "NOT_FOUND");
			case "INVALID_ARGUMENT":
			case "INPUT_TOO_LARGE":
				return e(status.INVALID_ARGUMENT, err.message);
			case "ALREADY_CLAIMED":
			case "NOT_CLAIMABLE":
			case "WORKER_IDENTITY_REUSED":
			case "STALE_EXECUTION":
				return e(status.FAILED_PRECONDITION, `STALE_EXECUTION: ${err.message}`);
			case "EFFECT_UNCERTAIN":
			case "PROFILE_UNQUALIFIED":
				return e(status.FAILED_PRECONDITION, err.message);
			case "FORBIDDEN":
				return e(status.PERMISSION_DENIED, err.message);
		}
	}
	return e(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
}

function generationOf(s: string): number {
	if (!/^[1-9][0-9]{0,18}$/.test(s)) throw new TaskError("INVALID_ARGUMENT", `generation ${s}`);
	return Number(s);
}

/** Validates a decoded ts-proto request through the contract boundary. */
function validated(typeName: string, json: unknown): void {
	const v = validateJson(typeName, JSON.stringify(json));
	if (!v.valid)
		throw Object.assign(
			new Error(
				`INVALID_ARGUMENT: ${v.reason === "invalid" ? v.violations.map((x) => x.message).join("; ") : v.error.message}`,
			),
			{
				code: status.INVALID_ARGUMENT,
			},
		);
}

export interface GrpcServer {
	listen(): Promise<number>;
	serve(): void;
	withdraw(): void;
	/** Drains within the timeout, then forces; reports whether force was needed. */
	stop(timeoutMs: number): Promise<boolean>;
}

/** The bounded listener: calls beyond capacity are refused with CAPACITY_EXHAUSTED. */
export function createGrpcServer(
	listen: string,
	capacity: number,
	tasks: Tasks,
	log: { error(msg: string, f?: Record<string, string>): void },
	sources?: Sources,
	ingest?: Ingest,
	indexer?: Indexer,
	snapshots?: Snapshots,
	retrieval?: Retrieval,
	memory?: Memory,
	projector?: MemoryProjector,
	recall?: Recall,
): GrpcServer {
	const server = new Server();
	const health = new HealthImplementation({ "": "NOT_SERVING" });
	health.addToServer(server);
	let inFlight = 0;
	const guard = <Req, Res>(
		typeName: string,
		toJson: (r: Req) => unknown,
		fn: (r: Req) => Promise<Res>,
	): ((call: ServerUnaryCall<Req, Res>, cb: sendUnaryData<Res>) => void) => {
		return (call, cb) => {
			if (inFlight >= capacity) {
				cb(
					Object.assign(new Error("CAPACITY_EXHAUSTED"), {
						code: status.RESOURCE_EXHAUSTED,
					}) as unknown as ServiceError,
					null,
				);
				return;
			}
			inFlight++;
			void (async () => {
				try {
					validated(typeName, toJson(call.request));
					cb(null, await fn(call.request));
				} catch (err) {
					if (
						typeof err === "object" &&
						err !== null &&
						"code" in err &&
						typeof (err as { code: unknown }).code === "number"
					)
						cb(err as ServiceError, null);
					else {
						if (
							!(err instanceof TaskError) &&
							!(err instanceof domain.SourceError) &&
							!(err instanceof ParseError) &&
							!(err instanceof IndexError) &&
							!(err instanceof RetrievalError) &&
							!(err instanceof MemoryError)
						)
							log.error("unmapped knowledge error", { error: String(err) });
						cb(toStatus(err), null);
					}
				} finally {
					inFlight--;
				}
			})();
		};
	};
	const impl: BackgroundTaskServiceServer = {
		claimTask: guard(
			"anvilkit.knowledge.v1.ClaimTaskRequest",
			ClaimTaskRequest.toJSON,
			async (req): Promise<ClaimTaskResponse> => {
				const out = await tasks.claim(req.taskId, generationOf(req.generation), req.workerId, req.leaseSeconds * 1000);
				return {
					$type: "anvilkit.knowledge.v1.ClaimTaskResponse",
					task: toProto(out.task),
					input: Buffer.from(out.input),
				};
			},
		),
		heartbeatTask: guard(
			"anvilkit.knowledge.v1.HeartbeatTaskRequest",
			HeartbeatTaskRequest.toJSON,
			async (req): Promise<HeartbeatTaskResponse> => {
				const task = await tasks.heartbeat(req.taskId, generationOf(req.generation), req.workerId);
				return { $type: "anvilkit.knowledge.v1.HeartbeatTaskResponse", task: toProto(task) };
			},
		),
		submitTaskResult: guard(
			"anvilkit.knowledge.v1.SubmitTaskResultRequest",
			SubmitTaskResultRequest.toJSON,
			async (req): Promise<SubmitTaskResultResponse> => {
				const d = await tasks.submit(req.taskId, generationOf(req.generation), {
					workerId: req.workerId,
					inputDigest: req.inputDigest,
					succeeded: req.succeeded,
					resultRef: req.resultRef,
					resultDigest: req.resultDigest,
					failureCode: req.failureCode ?? "",
				});
				return {
					$type: "anvilkit.knowledge.v1.SubmitTaskResultResponse",
					task: toProto(d.task),
					accepted: d.accepted,
					existing: d.existing,
				};
			},
		),
		getTask: guard(
			"anvilkit.knowledge.v1.GetTaskRequest",
			GetTaskRequest.toJSON,
			async (req): Promise<GetTaskResponse> => {
				return { $type: "anvilkit.knowledge.v1.GetTaskResponse", task: toProto(await tasks.get(req.taskId)) };
			},
		),
	};
	server.addService(BackgroundTaskServiceService, impl);
	if (sources) {
		const src: SourceServiceServer = {
			registerSource: guard(
				"anvilkit.knowledge.v1.RegisterSourceRequest",
				RegisterSourceRequest.toJSON,
				async (req): Promise<RegisterSourceResponse> => {
					const kind = kindOf[req.kind];
					if (!kind) throw new domain.SourceError("INVALID_ARGUMENT", "kind");
					const out = await sources.register(
						commandOf(req.command),
						scopeOf(req.scope),
						{
							kind,
							locator: req.locator,
							contentDigest: req.contentDigest,
							mediaType: req.mediaType,
							sizeBytes: req.sizeBytes,
						},
						accessOf(req.access),
					);
					return {
						$type: "anvilkit.knowledge.v1.RegisterSourceResponse",
						source: sourceToProto(out.source),
						ingestRequestId: out.ingestRequestId,
						existing: out.existing,
					};
				},
			),
			getSource: guard(
				"anvilkit.knowledge.v1.GetSourceRequest",
				GetSourceRequest.toJSON,
				async (req): Promise<GetSourceResponse> => ({
					$type: "anvilkit.knowledge.v1.GetSourceResponse",
					source: sourceToProto(await sources.get(scopeOf(req.scope), req.sourceId)),
				}),
			),
			listSources: guard(
				"anvilkit.knowledge.v1.ListSourcesRequest",
				ListSourcesRequest.toJSON,
				async (req): Promise<ListSourcesResponse> => {
					const out = await sources.list(scopeOf(req.scope), req.cursor, req.limit);
					return {
						$type: "anvilkit.knowledge.v1.ListSourcesResponse",
						sources: out.sources.map(sourceToProto),
						nextCursor: out.nextCursor,
					};
				},
			),
			updateSourceAccess: guard(
				"anvilkit.knowledge.v1.UpdateSourceAccessRequest",
				UpdateSourceAccessRequest.toJSON,
				async (req): Promise<UpdateSourceAccessResponse> => {
					const out = await sources.updateAccess(
						commandOf(req.command),
						scopeOf(req.scope),
						req.sourceId,
						sequenceOf(req.expectedAclRevision, "expected ACL revision"),
						accessOf(req.access),
					);
					return {
						$type: "anvilkit.knowledge.v1.UpdateSourceAccessResponse",
						source: sourceToProto(out.source),
						existing: out.existing,
					};
				},
			),
			deleteSource: guard(
				"anvilkit.knowledge.v1.DeleteSourceRequest",
				DeleteSourceRequest.toJSON,
				async (req): Promise<DeleteSourceResponse> => {
					const out = await sources.delete(
						commandOf(req.command),
						scopeOf(req.scope),
						req.sourceId,
						sequenceOf(req.expectedRevision, "expected revision"),
					);
					return {
						$type: "anvilkit.knowledge.v1.DeleteSourceResponse",
						source: sourceToProto(out.source),
						existing: out.existing,
					};
				},
			),
		};
		server.addService(SourceServiceService, src);
	}
	if (ingest) {
		const states = {
			launched: ParseState.PARSE_STATE_LAUNCHED,
			running: ParseState.PARSE_STATE_RUNNING,
			completed: ParseState.PARSE_STATE_COMPLETED,
			failed: ParseState.PARSE_STATE_FAILED,
		} as const;
		const ing: IngestServiceServer = {
			advanceProjection: guard(
				"anvilkit.knowledge.v1.AdvanceProjectionRequest",
				AdvanceProjectionRequest.toJSON,
				async (req): Promise<AdvanceProjectionResponse> => {
					if (!projector) throw new MemoryError("UNAVAILABLE", "no memory projector in this build");
					const a = await projector.advance(req.taskId, generationOf(req.generation), req.workerId, req.inputDigest);
					return {
						$type: "anvilkit.knowledge.v1.AdvanceProjectionResponse",
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
				},
			),
			advanceParse: guard(
				"anvilkit.knowledge.v1.AdvanceParseRequest",
				AdvanceParseRequest.toJSON,
				async (req): Promise<AdvanceParseResponse> => {
					const a = await ingest.advance(req.taskId, generationOf(req.generation), req.workerId, req.inputDigest);
					return {
						$type: "anvilkit.knowledge.v1.AdvanceParseResponse",
						state: states[a.state],
						resultRef: a.state === "completed" ? a.resultRef : "",
						resultDigest: a.state === "completed" ? a.resultDigest : "",
						failureCode: a.state === "failed" ? a.failureCode : undefined,
						retryAfterMs: a.state === "launched" || a.state === "running" ? a.retryAfterMs : 0,
					};
				},
			),
			advanceIndex: guard(
				"anvilkit.knowledge.v1.AdvanceIndexRequest",
				AdvanceIndexRequest.toJSON,
				async (req): Promise<AdvanceIndexResponse> => {
					if (!indexer) throw new IndexError("UNAVAILABLE", "no index builder in this build");
					const a = await indexer.advance(req.taskId, generationOf(req.generation), req.workerId, req.inputDigest);
					return {
						$type: "anvilkit.knowledge.v1.AdvanceIndexResponse",
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
				},
			),
		};
		server.addService(IngestServiceService, ing);
	}
	if (snapshots) {
		const snap: SnapshotServiceServer = {
			createSnapshot: guard(
				"anvilkit.knowledge.v1.CreateSnapshotRequest",
				CreateSnapshotRequest.toJSON,
				async (req): Promise<CreateSnapshotResponse> => {
					const out = await snapshots.create(commandOf(req.command), scopeOf(req.scope), req.sourceIds);
					return {
						$type: "anvilkit.knowledge.v1.CreateSnapshotResponse",
						snapshot: snapshotToProto(out.snapshot),
						existing: out.existing,
					};
				},
			),
			getSnapshot: guard(
				"anvilkit.knowledge.v1.GetSnapshotRequest",
				GetSnapshotRequest.toJSON,
				async (req): Promise<GetSnapshotResponse> => ({
					$type: "anvilkit.knowledge.v1.GetSnapshotResponse",
					snapshot: snapshotToProto(await snapshots.get(scopeOf(req.scope), req.snapshotId)),
				}),
			),
		};
		server.addService(SnapshotServiceService, snap);
	}
	if (memory) {
		const mem: MemoryServiceServer = {
			proposeFact: guard(
				"anvilkit.knowledge.v1.ProposeFactRequest",
				ProposeFactRequest.toJSON,
				async (req): Promise<ProposeFactResponse> => {
					const origin = originOf[req.origin];
					if (!origin) throw new MemoryError("INVALID_ARGUMENT", "origin");
					const out = await memory.propose(commandOf(req.command), scopeOf(req.scope), {
						subjectType: req.subjectType,
						subjectId: req.subjectId,
						content: req.content,
						sourceRefs: req.sourceRefs,
						expiresAt: req.expiresAt ?? null,
						origin,
					});
					return {
						$type: "anvilkit.knowledge.v1.ProposeFactResponse",
						fact: factToProto(out.fact),
						existing: out.existing,
					};
				},
			),
			decideFact: guard(
				"anvilkit.knowledge.v1.DecideFactRequest",
				DecideFactRequest.toJSON,
				async (req): Promise<DecideFactResponse> => {
					const decision = decisionOf[req.decision];
					if (!decision) throw new MemoryError("INVALID_ARGUMENT", "decision");
					const out = await memory.decide(
						commandOf(req.command),
						scopeOf(req.scope),
						req.factId,
						sequenceOf(req.expectedRevision, "expected revision"),
						decision,
						req.reasonCode ?? "",
						req.expiresAt ?? null,
					);
					return {
						$type: "anvilkit.knowledge.v1.DecideFactResponse",
						fact: factToProto(out.fact),
						existing: out.existing,
					};
				},
			),
			getFact: guard(
				"anvilkit.knowledge.v1.GetFactRequest",
				GetFactRequest.toJSON,
				async (req): Promise<GetFactResponse> => ({
					$type: "anvilkit.knowledge.v1.GetFactResponse",
					fact: factToProto(await memory.get(scopeOf(req.scope), req.factId)),
				}),
			),
			listFacts: guard(
				"anvilkit.knowledge.v1.ListFactsRequest",
				ListFactsRequest.toJSON,
				async (req): Promise<ListFactsResponse> => {
					const out = await memory.list(
						scopeOf(req.scope),
						{ subjectType: req.subjectType, subjectId: req.subjectId, state: factStateOf[req.state] ?? "" },
						req.cursor,
						req.limit,
					);
					return {
						$type: "anvilkit.knowledge.v1.ListFactsResponse",
						facts: out.facts.map(factToProto),
						nextCursor: out.nextCursor,
					};
				},
			),
			deleteFact: guard(
				"anvilkit.knowledge.v1.DeleteFactRequest",
				DeleteFactRequest.toJSON,
				async (req): Promise<DeleteFactResponse> => {
					const out = await memory.delete(
						commandOf(req.command),
						scopeOf(req.scope),
						req.factId,
						sequenceOf(req.expectedRevision, "expected revision"),
					);
					return {
						$type: "anvilkit.knowledge.v1.DeleteFactResponse",
						fact: factToProto(out.fact),
						existing: out.existing,
					};
				},
			),
			recallFacts: guard(
				"anvilkit.knowledge.v1.RecallFactsRequest",
				RecallFactsRequest.toJSON,
				async (req): Promise<RecallFactsResponse> => {
					if (!recall) throw new MemoryError("UNAVAILABLE", "recall is not wired in this build");
					if (!req.deadline) throw new MemoryError("INVALID_ARGUMENT", "deadline is required");
					const out = await recall.recall({
						scope: scopeOf(req.scope),
						query: req.query,
						maxFacts: req.maxFacts,
						retrievalProfileId: req.retrievalProfileId,
						deadline: req.deadline,
					});
					return {
						$type: "anvilkit.knowledge.v1.RecallFactsResponse",
						facts: out.facts.map(factToProto),
						noAnswer: out.noAnswer,
						indexGeneration: String(out.indexGeneration),
					};
				},
			),
		};
		server.addService(MemoryServiceService, mem);
	}
	if (retrieval) {
		const ret: RetrievalServiceServer = {
			search: guard(
				"anvilkit.knowledge.v1.SearchRequest",
				SearchRequest.toJSON,
				async (req): Promise<SearchResponse> => {
					if (!req.deadline) throw new RetrievalError("INVALID_ARGUMENT", "deadline is required");
					const out = await retrieval.search({
						scope: scopeOf(req.scope),
						snapshotId: req.snapshotId,
						query: req.query,
						maxContextItems: req.maxContextItems,
						retrievalProfileId: req.retrievalProfileId,
						deadline: req.deadline,
					});
					return {
						$type: "anvilkit.knowledge.v1.SearchResponse",
						items: out.items.map((i) =>
							ContextItem.fromPartial({ citation: Citation.fromPartial(i.citation), text: i.text }),
						),
						noAnswer: out.noAnswer,
						indexGeneration: String(out.indexGeneration),
						authorizationRevision: out.authorizationRevision,
					};
				},
			),
		};
		server.addService(RetrievalServiceService, ret);
	}
	return {
		listen: () =>
			new Promise<number>((resolve, reject) => {
				server.bindAsync(listen, ServerCredentials.createInsecure(), (err, port) =>
					err ? reject(err) : resolve(port),
				);
			}),
		serve: () => health.setStatus("", "SERVING"),
		withdraw: () => health.setStatus("", "NOT_SERVING"),
		stop: (timeoutMs) =>
			new Promise<boolean>((resolve) => {
				health.setStatus("", "NOT_SERVING");
				const bound = setTimeout(() => {
					server.forceShutdown();
					resolve(true);
				}, timeoutMs);
				server.tryShutdown(() => {
					clearTimeout(bound);
					resolve(false);
				});
			}),
	};
}
