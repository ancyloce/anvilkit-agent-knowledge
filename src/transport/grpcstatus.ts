// Knowledge's error mapping: domain errors to the public gRPC codes of
// contracts.md §4; anything unmapped is DEPENDENCY_UNAVAILABLE.

import { type ServiceError, status } from "@grpc/grpc-js";
import { IndexError } from "../domain/index.js";
import { MemoryError } from "../domain/memory.js";
import { ParseError } from "../domain/parse.js";
import { RetrievalError } from "../domain/retrieval.js";
import * as domain from "../domain/source.js";
import { TaskError } from "../domain/task.js";

export function toStatus(err: unknown): ServiceError {
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
			case "UNAVAILABLE":
				return e(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
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
