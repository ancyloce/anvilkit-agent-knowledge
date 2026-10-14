// The EventEnvelope of contracts/events/events.schema.json with a reviewed
// small payload (identities, digests, states; never a body or a token).
// Knowledge publishes background.requested only: source.revision-indexed,
// source.authorization-revoked, memory.fact-confirmed/revoked and
// background.completed left the event catalog in P2.8 (no consumer; their
// projections run inside Knowledge's own transactions and background
// requests).
import { randomUUID } from "node:crypto";
import type { Task } from "./task.js";

export const producer = "anvilkit-agent-knowledge";
export const subjects = {
	backgroundRequested: "anvilkit.knowledge.background.requested",
} as const;

export interface Envelope {
	eventId: string;
	eventType: "background.requested";
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

/**
 * background.requested for a new generation. Each generation publishes it
 * once, so its aggregateRevision is the generation, which strictly
 * increases over the task's events (P2.8, MCP-14); the row revision
 * restarts with every generation and is not used.
 */
export function requestedEvent(t: Task, now: Date): Envelope {
	return {
		eventId: randomUUID(),
		eventType: "background.requested",
		schemaVersion: 1,
		producer,
		subject: subjects.backgroundRequested,
		tenantId: t.tenantId,
		aggregateType: "background_request",
		aggregateId: t.taskId,
		aggregateRevision: String(t.generation),
		occurredAt: now.toISOString(),
		correlationId: t.correlationId,
		payload: {
			kind: "background.requested",
			taskId: t.taskId,
			generation: String(t.generation),
			taskKind: t.kind,
			inputDigest: t.inputDigest,
		},
	};
}
