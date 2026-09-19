// The lane's Prometheus signals (DD-09 §6). Labels are controlled
// vocabularies; no identifier or body is a label value.
import { Counter, Gauge, type Registry } from "prom-client";

export class Metrics {
	readonly requests: Gauge<"state">;
	readonly claims: Counter<"outcome">;
	readonly submissions: Counter<"outcome">;
	readonly leaseOverruns: Counter;
	readonly overdueRetries: Gauge;
	readonly outboxOldest: Gauge;
	readonly configGeneration: Gauge;
	readonly configRejections: Counter;
	readonly configRotations: Counter;
	readonly drainSeconds: Gauge;
	readonly forcedStop: Gauge;
	readonly poolTotal: Gauge;
	readonly poolIdle: Gauge;
	readonly dispatchQueries: Counter<"answer">;

	constructor(registry: Registry) {
		const r = [registry];
		this.requests = new Gauge({
			name: "anvilkit_knowledge_background_requests",
			help: "Durable background requests by state.",
			labelNames: ["state"],
			registers: r,
		});
		this.claims = new Counter({
			name: "anvilkit_knowledge_background_claims_total",
			help: "Claim decisions by outcome.",
			labelNames: ["outcome"],
			registers: r,
		});
		this.submissions = new Counter({
			name: "anvilkit_knowledge_background_submissions_total",
			help: "Result submissions by outcome.",
			labelNames: ["outcome"],
			registers: r,
		});
		this.leaseOverruns = new Counter({
			name: "anvilkit_knowledge_background_lease_overruns_total",
			help: "Leases that expired before a result was submitted.",
			registers: r,
		});
		this.overdueRetries = new Gauge({
			name: "anvilkit_knowledge_background_overdue_retries",
			help: "retry_scheduled requests whose retry time has passed without a claim.",
			registers: r,
		});
		this.outboxOldest = new Gauge({
			name: "anvilkit_knowledge_outbox_oldest_unforwarded_seconds",
			help: "Age of the oldest outbox message the forwarder has not acknowledged.",
			registers: r,
		});
		this.configGeneration = new Gauge({
			name: "anvilkit_knowledge_config_generation",
			help: "Number of the active configuration generation.",
			registers: r,
		});
		this.configRejections = new Counter({
			name: "anvilkit_knowledge_config_rejections_total",
			help: "Candidate configuration generations rejected by validation, construction or probe.",
			registers: r,
		});
		this.configRotations = new Counter({
			name: "anvilkit_knowledge_config_rotations_total",
			help: "Generations published after a secret or snapshot change.",
			registers: r,
		});
		this.drainSeconds = new Gauge({
			name: "anvilkit_knowledge_shutdown_drain_seconds",
			help: "Seconds the last shutdown spent draining before dependencies closed.",
			registers: r,
		});
		this.forcedStop = new Gauge({
			name: "anvilkit_knowledge_shutdown_forced",
			help: "1 when the last shutdown had to force-stop a server or loop.",
			registers: r,
		});
		this.poolTotal = new Gauge({
			name: "anvilkit_knowledge_db_pool_total_connections",
			help: "Connections of the active generation's pool.",
			registers: r,
		});
		this.poolIdle = new Gauge({
			name: "anvilkit_knowledge_db_pool_idle_connections",
			help: "Idle connections of the active generation's pool.",
			registers: r,
		});
		this.dispatchQueries = new Counter({
			name: "anvilkit_knowledge_background_dispatch_queries_total",
			help: "Original-dispatch queries for expired external-effect leases by answer.",
			labelNames: ["answer"],
			registers: r,
		});
	}
}
