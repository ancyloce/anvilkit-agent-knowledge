// Knowledge's grpc-js transport (A03) for the owner surface of P14:
// anvilkit.knowledge.v1.BackgroundTaskService. Every decoded request passes
// the contract's explicit TypeScript validation (protovalidate over the
// generated descriptors) before its handler; domain errors map to the
// public codes of contracts.md §4. The listener is plaintext
// (DEVELOPMENT_ONLY; workload mTLS is ENV-03), like the other new services.
import {
	BackgroundTask,
	type BackgroundTaskServiceServer,
	BackgroundTaskServiceService,
	ClaimTaskRequest,
	type ClaimTaskResponse,
	GetTaskRequest,
	type GetTaskResponse,
	HeartbeatTaskRequest,
	type HeartbeatTaskResponse,
	SubmitTaskResultRequest,
	type SubmitTaskResultResponse,
	TaskState,
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
import type { Tasks } from "../application/tasks.js";
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

function toStatus(err: unknown): ServiceError {
	const e = (code: status, message: string) =>
		Object.assign(new Error(message), { code, details: message, metadata: undefined }) as unknown as ServiceError;
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
						if (!(err instanceof TaskError)) log.error("unmapped knowledge error", { error: String(err) });
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
