// Knowledge's grpc-js transport (A03): anvilkit.knowledge.v1
// BackgroundTaskService (P14), SourceService and IngestService (P15). Every
// decoded request passes
// the contract's explicit TypeScript validation (protovalidate over the
// generated descriptors) before its handler; domain errors map to the
// public codes of contracts.md §4. The listener is plaintext
// (DEVELOPMENT_ONLY; workload mTLS is ENV-03), like the other new services.
import {
	AccessEntry,
	AdvanceParseRequest,
	type AdvanceParseResponse,
	BackgroundTask,
	type BackgroundTaskServiceServer,
	BackgroundTaskServiceService,
	ClaimTaskRequest,
	type ClaimTaskResponse,
	DeleteSourceRequest,
	type DeleteSourceResponse,
	GetSourceRequest,
	type GetSourceResponse,
	GetTaskRequest,
	type GetTaskResponse,
	HeartbeatTaskRequest,
	type HeartbeatTaskResponse,
	type IngestServiceServer,
	IngestServiceService,
	IngestState,
	ListSourcesRequest,
	type ListSourcesResponse,
	ParseState,
	RegisterSourceRequest,
	type RegisterSourceResponse,
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
import type { Ingest } from "../application/ingest.js";
import type { Sources } from "../application/sources.js";
import type { Tasks } from "../application/tasks.js";
import { ParseError } from "../domain/parse.js";
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
						if (!(err instanceof TaskError) && !(err instanceof domain.SourceError) && !(err instanceof ParseError))
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
		};
		server.addService(IngestServiceService, ing);
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
