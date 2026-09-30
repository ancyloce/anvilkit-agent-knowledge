// The EventEnvelope of contracts/events/events.schema.json with a reviewed
// small payload (identities, digests, states; never a body or a token).
import { randomUUID } from "node:crypto";
import type { Task } from "./task.js";

export const producer = "anvilkit-agent-knowledge";
export const subjects = {
	backgroundRequested: "anvilkit.knowledge.background.requested",
	backgroundCompleted: "anvilkit.knowledge.background.completed",
	sourceAuthorizationRevoked: "anvilkit.knowledge.source.authorization-revoked",
	sourceRevisionIndexed: "anvilkit.knowledge.source.revision-indexed",
	memoryFactConfirmed: "anvilkit.knowledge.memory.fact-confirmed",
	memoryFactRevoked: "anvilkit.knowledge.memory.fact-revoked",
} as const;

export interface Envelope {
	eventId: string;
	eventType:
		| "background.requested"
		| "background.completed"
		| "source.authorization-revoked"
		| "source.revision-indexed"
		| "memory.fact-confirmed"
		| "memory.fact-revoked";
	schemaVersion: 1;
	producer: typeof producer;
	subject: string;
	tenantId: string;
	aggregateType: "background_request" | "source" | "memory_fact";
	aggregateId: string;
	aggregateRevision: string;
	occurredAt: string;
	correlationId: string;
	payload: Record<string, string>;
}

function envelope(
	t: Task,
	eventType: Envelope["eventType"],
	subject: string,
	now: Date,
	payload: Record<string, string>,
): Envelope {
	return {
		eventId: randomUUID(),
		eventType,
		schemaVersion: 1,
		producer,
		subject,
		tenantId: t.tenantId,
		aggregateType: "background_request",
		aggregateId: t.taskId,
		aggregateRevision: String(t.revision),
		occurredAt: now.toISOString(),
		correlationId: t.correlationId,
		payload,
	};
}

export function requestedEvent(t: Task, now: Date): Envelope {
	return envelope(t, "background.requested", subjects.backgroundRequested, now, {
		kind: "background.requested",
		taskId: t.taskId,
		generation: String(t.generation),
		taskKind: t.kind,
		inputDigest: t.inputDigest,
	});
}

export function completedEvent(t: Task, now: Date): Envelope {
	const payload: Record<string, string> = {
		kind: "background.completed",
		taskId: t.taskId,
		generation: String(t.generation),
		state: t.state,
	};
	if (t.state === "accepted") payload.resultDigest = t.resultDigest;
	return envelope(t, "background.completed", subjects.backgroundCompleted, now, payload);
}

/**
 * A committed revocation (ACL entries removed or the source deleted): the
 * invalidation consumers stop disclosing the source's content under the
 * previous ACL. The payload carries identities and the ACL revision only.
 */
export function sourceRevokedEvent(
	s: { sourceId: string; tenantId: string; aclRevision: number; revision: number },
	correlationId: string,
	now: Date,
): Envelope {
	return {
		eventId: randomUUID(),
		eventType: "source.authorization-revoked",
		schemaVersion: 1,
		producer,
		subject: subjects.sourceAuthorizationRevoked,
		tenantId: s.tenantId,
		aggregateType: "source",
		aggregateId: s.sourceId,
		aggregateRevision: String(s.revision),
		occurredAt: now.toISOString(),
		correlationId,
		payload: { kind: "source.authorization-revoked", sourceId: s.sourceId, aclRevision: String(s.aclRevision) },
	};
}

/**
 * A source revision became readable through the accepted index generation
 * (its entry there was accepted). Identities and sequence numbers only.
 */
export function sourceIndexedEvent(
	s: { sourceId: string; tenantId: string; sourceRevision: number; indexGeneration: number },
	correlationId: string,
	now: Date,
): Envelope {
	return {
		eventId: randomUUID(),
		eventType: "source.revision-indexed",
		schemaVersion: 1,
		producer,
		subject: subjects.sourceRevisionIndexed,
		tenantId: s.tenantId,
		aggregateType: "source",
		aggregateId: s.sourceId,
		aggregateRevision: String(s.sourceRevision),
		occurredAt: now.toISOString(),
		correlationId,
		payload: {
			kind: "source.revision-indexed",
			sourceId: s.sourceId,
			sourceRevision: String(s.sourceRevision),
			indexGeneration: String(s.indexGeneration),
		},
	};
}

/**
 * A committed memory decision that changes recall: memory.fact-confirmed, or
 * memory.fact-revoked with the state that removed the fact (revoked,
 * expired or deleted). Identities, revision and state only; never content.
 */
export function memoryFactEvent(
	f: { factId: string; tenantId: string; revision: number },
	state: "confirmed" | "revoked" | "expired" | "deleted",
	correlationId: string,
	now: Date,
): Envelope {
	const confirmed = state === "confirmed";
	return {
		eventId: randomUUID(),
		eventType: confirmed ? "memory.fact-confirmed" : "memory.fact-revoked",
		schemaVersion: 1,
		producer,
		subject: confirmed ? subjects.memoryFactConfirmed : subjects.memoryFactRevoked,
		tenantId: f.tenantId,
		aggregateType: "memory_fact",
		aggregateId: f.factId,
		aggregateRevision: String(f.revision),
		occurredAt: now.toISOString(),
		correlationId,
		payload: {
			kind: confirmed ? "memory.fact-confirmed" : "memory.fact-revoked",
			factId: f.factId,
			revision: String(f.revision),
			state,
		},
	};
}
