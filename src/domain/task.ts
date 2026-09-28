// The owner's rules for durable background work (DD-09 §1): states and
// every transition decision as pure functions of the current row, the
// request and the clock. The same rules as the MCP owner (Go); the
// application layer executes one decision inside one transaction.
import { createHash } from "node:crypto";
import { ingestProfile, parseIngestInput, SourceError } from "./source.js";

export type TaskState =
	| "pending"
	| "leased"
	| "result_submitted"
	| "accepted"
	| "retry_scheduled"
	| "dead"
	| "stale"
	| "canceled";

export const terminal = (s: TaskState): boolean =>
	s === "accepted" || s === "dead" || s === "stale" || s === "canceled";

export type Effects = "reconstructible" | "external";

/** The task kinds this owner's schema admits and the fixture kind of the lane. */
export const kinds = ["knowledge-ingest", "knowledge-project", "memory-project", "local-check"] as const;
export type TaskKind = (typeof kinds)[number];

/** The DEVELOPMENT_ONLY fixture profile: the result digest is the SHA-256 of the declared bytes. */
export const localCheckProfile = "local-check-v1";

export const failure = {
	leaseExpired: "LEASE_EXPIRED",
	effectUncertain: "EFFECT_UNCERTAIN",
	profileMismatch: "PROFILE_MISMATCH",
	authorizationRevoked: "AUTHORIZATION_REVOKED",
	canceled: "CANCELED",
	superseded: "SUPERSEDED",
	attemptsExhausted: "ATTEMPTS_EXHAUSTED",
	handler: "HANDLER_FAILED",
} as const;

export const attemptOutcome = {
	submitted: "submitted",
	accepted: "accepted",
	stale: "stale",
	expired: "expired",
	failed: "failed",
} as const;

/** Errors of the decisions; the transport maps them to gRPC codes. */
export class TaskError extends Error {
	constructor(
		readonly code:
			| "NOT_FOUND"
			| "INVALID_ARGUMENT"
			| "NOT_CLAIMABLE"
			| "STALE_EXECUTION"
			| "EFFECT_UNCERTAIN"
			| "PROFILE_UNQUALIFIED"
			| "ALREADY_CLAIMED"
			| "INPUT_TOO_LARGE"
			| "WORKER_IDENTITY_REUSED"
			| "FORBIDDEN",
		message: string,
	) {
		super(`${code}: ${message}`);
	}
}

export interface Task {
	taskId: string;
	generation: number;
	tenantId: string;
	kind: TaskKind;
	inputDigest: string;
	input: string; // the canonical JSON text a claim returns
	state: TaskState;
	workerId: string;
	leaseUntil: Date | null;
	attemptCount: number;
	maxAttempts: number;
	resultProfile: string;
	effects: Effects;
	dispatchId: string;
	authorizationRef: string;
	retryAt: Date | null;
	resultRef: string;
	resultDigest: string;
	failureCode: string;
	revision: number;
	correlationId: string;
}

export interface Attempt {
	ordinal: number;
	workerId: string;
	leasedAt: Date;
	leaseUntil: Date;
	outcome: string;
}

export interface Bounds {
	maxInputBytes: number;
	maxLeaseMs: number;
	retryDelayMs: number;
	maxAttempts: number;
}

export function digestOf(data: Buffer | string): string {
	return `sha256:${createHash("sha256").update(data).digest("hex")}`;
}

const digestPattern = /^sha256:[0-9a-f]{64}$/;

/** The fixture's frozen input (DEVELOPMENT_ONLY): bytes and scenario switches, never a business parser. */
export interface LocalCheckInput {
	schemaVersion: 1;
	computation: typeof localCheckProfile;
	bytes: string;
	holdMs?: number;
	fail?: boolean;
	wrongDigest?: boolean;
}

function parseLocalCheck(input: string): LocalCheckInput {
	let raw: unknown;
	try {
		raw = JSON.parse(input);
	} catch {
		throw new TaskError("INVALID_ARGUMENT", "local-check input is not JSON");
	}
	if (typeof raw !== "object" || raw === null || Array.isArray(raw))
		throw new TaskError("INVALID_ARGUMENT", "local-check input must be an object");
	const r = raw as Record<string, unknown>;
	if (r.schemaVersion !== 1 || r.computation !== localCheckProfile)
		throw new TaskError(
			"INVALID_ARGUMENT",
			`local-check input must declare schemaVersion 1 and computation ${localCheckProfile}`,
		);
	if (typeof r.bytes !== "string" || !/^[A-Za-z0-9+/]*={0,2}$/.test(r.bytes))
		throw new TaskError("INVALID_ARGUMENT", "local-check bytes are not base64");
	return r as unknown as LocalCheckInput;
}

/** Validates a new generation; unknown profiles and oversized inputs are refused before storage. */
export function newRequest(
	p: {
		taskId: string;
		tenantId: string;
		kind: string;
		profile: string;
		input: string;
		effects: Effects;
		dispatchId: string;
		authorizationRef: string;
		correlationId: string;
	},
	bounds: Bounds,
): Task {
	if (!p.taskId || !p.tenantId || !p.correlationId)
		throw new TaskError("INVALID_ARGUMENT", "task, tenant and correlation identities are required");
	if (!(kinds as readonly string[]).includes(p.kind))
		throw new TaskError("INVALID_ARGUMENT", `unknown task kind ${p.kind}`);
	if (Buffer.byteLength(p.input) > bounds.maxInputBytes)
		throw new TaskError(
			"INPUT_TOO_LARGE",
			`${Buffer.byteLength(p.input)} bytes over the ${bounds.maxInputBytes}-byte bound`,
		);
	if (p.effects !== "reconstructible" && p.effects !== "external")
		throw new TaskError("INVALID_ARGUMENT", `effects ${String(p.effects)}`);
	if (p.effects === "external" && !p.dispatchId)
		throw new TaskError("INVALID_ARGUMENT", "an external-effect request names its Control dispatch");
	if (p.profile === localCheckProfile) parseLocalCheck(p.input);
	else if (p.profile === ingestProfile) {
		if (p.kind !== "knowledge-ingest")
			throw new TaskError("INVALID_ARGUMENT", `${ingestProfile} belongs to knowledge-ingest tasks`);
		if (p.effects !== "reconstructible")
			throw new TaskError("INVALID_ARGUMENT", "parsing is reconstructible computation");
		try {
			const input = parseIngestInput(p.input);
			if (p.authorizationRef !== `source:${input.sourceId}`)
				throw new TaskError("INVALID_ARGUMENT", "an ingest request is authorized by its own source");
		} catch (err) {
			if (err instanceof SourceError) throw new TaskError("INVALID_ARGUMENT", err.message);
			throw err;
		}
	} else throw new TaskError("PROFILE_UNQUALIFIED", `result profile ${p.profile} has no acceptance rule in this build`);
	return {
		taskId: p.taskId,
		generation: 1,
		tenantId: p.tenantId,
		kind: p.kind as TaskKind,
		inputDigest: digestOf(p.input),
		input: p.input,
		state: "pending",
		workerId: "",
		leaseUntil: null,
		attemptCount: 0,
		maxAttempts: bounds.maxAttempts,
		resultProfile: p.profile,
		effects: p.effects,
		dispatchId: p.dispatchId,
		authorizationRef: p.authorizationRef,
		retryAt: null,
		resultRef: "",
		resultDigest: "",
		failureCode: "",
		revision: 1,
		correlationId: p.correlationId,
	};
}

/** The result a profile requires: recomputed from the frozen input, or the owner's own verified record. */
export interface ExpectedResult {
	ref: string;
	digest: string;
}

/** The result reference and digest of the local-check profile, recomputed from the frozen input. */
export function expectedResult(t: Task): ExpectedResult {
	if (t.resultProfile !== localCheckProfile) throw new TaskError("PROFILE_UNQUALIFIED", t.resultProfile);
	const input = parseLocalCheck(t.input);
	return { ref: `local-check:${t.taskId}:${t.generation}`, digest: digestOf(Buffer.from(input.bytes, "base64")) };
}

/** Fences the previous generation when a new one is created. */
export function supersede(prev: Task): { task: Task; changed: boolean } {
	if (terminal(prev.state)) return { task: prev, changed: false };
	return {
		task: { ...prev, state: "stale", failureCode: failure.superseded, revision: prev.revision + 1 },
		changed: true,
	};
}

export interface ClaimDecision {
	task: Task;
	attempt: Attempt;
	expiredPrevious: boolean;
}

/**
 * Admits one claimant: pending or due retry_scheduled rows, and a leased
 * row whose lease expired when the computation is reconstructible. A worker
 * identity claims a generation once (the identity reuse fence).
 */
export function decideClaim(
	t: Task,
	previous: Attempt[],
	workerId: string,
	leaseMs: number,
	now: Date,
	bounds: Bounds,
): ClaimDecision {
	if (leaseMs <= 0 || leaseMs > bounds.maxLeaseMs)
		throw new TaskError("INVALID_ARGUMENT", `lease ${leaseMs}ms outside (0, ${bounds.maxLeaseMs}ms]`);
	for (const a of previous)
		if (a.workerId === workerId)
			throw new TaskError(
				"WORKER_IDENTITY_REUSED",
				`${workerId} already claimed generation ${t.generation} as attempt ${a.ordinal}`,
			);
	let expiredPrevious = false;
	switch (t.state) {
		case "pending":
			break;
		case "retry_scheduled":
			if (t.retryAt && t.retryAt.getTime() > now.getTime())
				throw new TaskError("NOT_CLAIMABLE", `retry scheduled at ${t.retryAt.toISOString()}`);
			break;
		case "leased":
			if (t.leaseUntil && t.leaseUntil.getTime() > now.getTime())
				throw new TaskError("ALREADY_CLAIMED", `leased by another worker until ${t.leaseUntil.toISOString()}`);
			if (t.effects !== "reconstructible")
				throw new TaskError(
					"EFFECT_UNCERTAIN",
					"lease expired on an external-effect task; the owner queries the original dispatch before any reassignment",
				);
			expiredPrevious = true;
			break;
		default:
			throw new TaskError("NOT_CLAIMABLE", `state ${t.state}`);
	}
	if (t.attemptCount >= t.maxAttempts)
		throw new TaskError("NOT_CLAIMABLE", `${t.attemptCount} of ${t.maxAttempts} attempts used`);
	const leaseUntil = new Date(now.getTime() + leaseMs);
	const task: Task = {
		...t,
		state: "leased",
		workerId,
		leaseUntil,
		attemptCount: t.attemptCount + 1,
		retryAt: null,
		failureCode: "",
		revision: t.revision + 1,
	};
	return {
		task,
		attempt: { ordinal: task.attemptCount, workerId, leasedAt: now, leaseUntil, outcome: "" },
		expiredPrevious,
	};
}

/** Extends a valid lease of the current claimant by the attempt's own lease length. */
export function decideHeartbeat(t: Task, current: Attempt | undefined, workerId: string, now: Date): Task {
	if (t.state !== "leased" || t.workerId !== workerId || !current || current.workerId !== workerId)
		throw new TaskError("STALE_EXECUTION", `not the current claimant (state ${t.state})`);
	if (!t.leaseUntil || t.leaseUntil.getTime() <= now.getTime())
		throw new TaskError("STALE_EXECUTION", `lease expired at ${t.leaseUntil?.toISOString() ?? "?"}`);
	const length = current.leaseUntil.getTime() - current.leasedAt.getTime();
	return { ...t, leaseUntil: new Date(now.getTime() + length), revision: t.revision + 1 };
}

export interface Submission {
	workerId: string;
	inputDigest: string;
	succeeded: boolean;
	resultRef: string;
	resultDigest: string;
	failureCode: string;
}

export type Authorization = "current" | "revoked";

export interface SubmitDecision {
	task: Task;
	attemptOutcome: string;
	accepted: boolean;
	existing: boolean;
	changed: boolean;
}

function failAttempt(t: Task, code: string, now: Date, bounds: Bounds): SubmitDecision {
	if (t.attemptCount >= t.maxAttempts)
		return {
			task: { ...t, state: "dead", failureCode: code, revision: t.revision + 1 },
			attemptOutcome: attemptOutcome.failed,
			accepted: false,
			existing: false,
			changed: true,
		};
	return {
		task: {
			...t,
			state: "retry_scheduled",
			failureCode: code,
			retryAt: new Date(now.getTime() + bounds.retryDelayMs),
			workerId: "",
			revision: t.revision + 1,
		},
		attemptOutcome: attemptOutcome.failed,
		accepted: false,
		existing: false,
		changed: true,
	};
}

/**
 * Accepts or refuses a result under the owner's CAS: current generation,
 * current claimant with a valid lease, frozen input digest, result profile
 * and current authorization. A repeated identical submission returns the
 * existing acceptance; a superseded or expired claimant is refused without
 * touching the row; a digest mismatch is a caller error; a profile mismatch
 * or a reported failure consumes the attempt; a revoked authorization
 * cancels the request.
 */
export function decideSubmit(
	t: Task,
	current: Attempt | undefined,
	sub: Submission,
	auth: Authorization,
	now: Date,
	bounds: Bounds,
	recorded?: ExpectedResult,
): SubmitDecision {
	if (!digestPattern.test(sub.inputDigest)) throw new TaskError("INVALID_ARGUMENT", "input digest");
	if (t.state === "accepted") {
		if (
			t.workerId === sub.workerId &&
			t.resultDigest === sub.resultDigest &&
			t.resultRef === sub.resultRef &&
			sub.succeeded
		)
			return { task: t, attemptOutcome: "", accepted: true, existing: true, changed: false };
		throw new TaskError("STALE_EXECUTION", `generation ${t.generation} already accepted a result`);
	}
	if (t.state !== "leased" || t.workerId !== sub.workerId || !current || current.workerId !== sub.workerId)
		throw new TaskError("STALE_EXECUTION", `not the current claimant (state ${t.state})`);
	if (!t.leaseUntil || t.leaseUntil.getTime() <= now.getTime())
		throw new TaskError("STALE_EXECUTION", `lease expired at ${t.leaseUntil?.toISOString() ?? "?"}`);
	if (sub.inputDigest !== t.inputDigest)
		throw new TaskError("INVALID_ARGUMENT", "input digest does not match the frozen input");
	if (auth === "revoked")
		return {
			task: { ...t, state: "canceled", failureCode: failure.authorizationRevoked, revision: t.revision + 1 },
			attemptOutcome: attemptOutcome.stale,
			accepted: false,
			existing: false,
			changed: true,
		};
	if (!sub.succeeded) return failAttempt(t, sub.failureCode || failure.handler, now, bounds);
	if (!digestPattern.test(sub.resultDigest))
		throw new TaskError("INVALID_ARGUMENT", "a succeeded result carries its digest");
	// Profiles whose result cannot be recomputed from the input (the parser's
	// output) are compared with the owner's verified record of this attempt;
	// without one the submission cannot match.
	const expected = t.resultProfile === localCheckProfile ? expectedResult(t) : recorded;
	if (!expected || sub.resultRef !== expected.ref || sub.resultDigest !== expected.digest)
		return failAttempt(t, failure.profileMismatch, now, bounds);
	return {
		task: {
			...t,
			state: "accepted",
			resultRef: sub.resultRef,
			resultDigest: sub.resultDigest,
			failureCode: "",
			revision: t.revision + 1,
		},
		attemptOutcome: attemptOutcome.accepted,
		accepted: true,
		existing: false,
		changed: true,
	};
}

/** Cancels a non-terminal generation; a leased attempt is recorded stale. */
export function decideCancel(t: Task): { task: Task; attemptOutcome: string; changed: boolean } {
	if (terminal(t.state)) return { task: t, attemptOutcome: "", changed: false };
	return {
		task: { ...t, state: "canceled", failureCode: failure.canceled, revision: t.revision + 1 },
		attemptOutcome: t.state === "leased" ? attemptOutcome.stale : "",
		changed: true,
	};
}

export type DispatchOutcome = "unknown" | "not_sent" | "sent";

/**
 * Handles a lease that ran out: reconstructible computation is rescheduled
 * while attempts remain and dead afterwards; an external-effect lease is
 * released only with not-sent evidence, otherwise dead with
 * EFFECT_UNCERTAIN and nothing is resent.
 */
export function decideExpire(
	t: Task,
	outcome: DispatchOutcome,
	now: Date,
	bounds: Bounds,
): { task: Task; attemptOutcome: string; changed: boolean } {
	if (t.state !== "leased" || !t.leaseUntil || t.leaseUntil.getTime() > now.getTime())
		return { task: t, attemptOutcome: "", changed: false };
	if (t.effects === "external" && outcome !== "not_sent")
		return {
			task: { ...t, state: "dead", failureCode: failure.effectUncertain, revision: t.revision + 1 },
			attemptOutcome: attemptOutcome.expired,
			changed: true,
		};
	if (t.attemptCount >= t.maxAttempts)
		return {
			task: { ...t, state: "dead", failureCode: failure.attemptsExhausted, workerId: "", revision: t.revision + 1 },
			attemptOutcome: attemptOutcome.expired,
			changed: true,
		};
	return {
		task: {
			...t,
			state: "retry_scheduled",
			retryAt: new Date(now.getTime() + bounds.retryDelayMs),
			failureCode: failure.leaseExpired,
			workerId: "",
			revision: t.revision + 1,
		},
		attemptOutcome: attemptOutcome.expired,
		changed: true,
	};
}
