// The EventEnvelope of contracts/events/events.schema.json with a reviewed
// small payload (identities, digests, states; never a body or a token).
import { randomUUID } from "node:crypto";
import type { Task } from "./task.js";

export const producer = "anvilkit-agent-knowledge";
export const subjects = {
	backgroundRequested: "anvilkit.knowledge.background.requested",
	backgroundCompleted: "anvilkit.knowledge.background.completed",
} as const;

export interface Envelope {
	eventId: string;
	eventType: "background.requested" | "background.completed";
	schemaVersion: 1;
	producer: typeof producer;
	subject: string;
	tenantId: string;
	aggregateType: "background_request";
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
