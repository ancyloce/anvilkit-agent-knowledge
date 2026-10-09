// The workloads that may call Knowledge (architecture.md communication
// matrix; docs/plans/0001 P0.1 allowlist). The namespace is part of the
// identity. A method without an entry is denied and fails the completeness
// check at startup.
import { anyAuthenticated, type Policy, type Workload } from "./identity.js";

export const api: Workload = { namespace: "anvilkit-apps", serviceAccount: "anvilkit-agent-api" };
const backgroundWorker: Workload = { namespace: "anvilkit-apps", serviceAccount: "anvilkit-agent-background-worker" };
const knowledge: Workload = { namespace: "anvilkit-apps", serviceAccount: "anvilkit-agent-knowledge" };
const sidecar: Workload = { namespace: "anvilkit-components", serviceAccount: "anvilkit-job-access-sidecar" };

export function policy(): Policy {
	const p: Policy = {};
	const add = (service: string, methods: string[], ...allowed: Workload[]) => {
		for (const m of methods) p[`/anvilkit.knowledge.v1.${service}/${m}`] = allowed;
	};
	add("SourceService", ["RegisterSource", "GetSource", "ListSources", "UpdateSourceAccess", "DeleteSource"], api);
	add("SnapshotService", ["CreateSnapshot", "GetSnapshot"], api);
	add("MemoryService", ["ProposeFact", "DecideFact", "GetFact", "ListFacts", "DeleteFact", "RecallFacts"], api);
	add("RetrievalService", ["Search"], api, sidecar);
	// The lane: the Background Worker and the relay container of the
	// Knowledge Pod (which carries the Pod's own workload identity).
	add(
		"BackgroundTaskService",
		["ClaimTask", "HeartbeatTask", "SubmitTaskResult", "GetTask"],
		backgroundWorker,
		knowledge,
	);
	add("IngestService", ["AdvanceParse", "AdvanceIndex", "AdvanceProjection"], backgroundWorker);
	for (const m of ["Check", "Watch", "List"]) p[`/grpc.health.v1.Health/${m}`] = [anyAuthenticated];
	return p;
}
