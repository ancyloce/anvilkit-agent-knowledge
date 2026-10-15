// The plaintext probe and metrics listener: /healthz answers 200 while the
// process runs, /readyz 200 only while the gRPC listener serves, the schema
// is at or above the binary's minimum and no shutdown has started, /metrics
// the Prometheus exposition. First up, last down, so a scrape during the
// drain still sees the shutdown gauges.
import { createServer, type Server } from "node:http";
import type { Registry } from "prom-client";
import type { Logger } from "../log.js";

export function createHealthServer(
	registry: Registry,
	isReady: () => boolean,
	notReadyReason: () => string | undefined = () => undefined,
): Server {
	return createServer((req, res) => {
		if (req.url === "/healthz") {
			res.writeHead(200).end();
			return;
		}
		if (req.url === "/readyz") {
			if (isReady()) {
				res.writeHead(200).end();
				return;
			}
			res.writeHead(503).end(notReadyReason() ?? "");
			return;
		}
		if (req.url === "/metrics") {
			void registry.metrics().then((body) => {
				res.writeHead(200, { "content-type": registry.contentType }).end(body);
			});
			return;
		}
		res.writeHead(404).end();
	});
}

export function listen(server: Server, address: string): Promise<void> {
	const i = address.lastIndexOf(":");
	const host = address.slice(0, i);
	const port = Number(address.slice(i + 1));
	return new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(port, host, () => resolve());
	});
}

/**
 * P2.7: a dependency's last known state for readiness, refreshed every
 * intervalMs by one bounded check at a time (never overlapping). Not ready
 * before the first check succeeds and while the last one failed or did not
 * finish within timeoutMs; ready again as soon as one succeeds.
 */
export class DependencyProbe {
	private ok = false;
	private running = false;
	private timer: NodeJS.Timeout | undefined;

	constructor(
		private readonly check: () => Promise<void>,
		private readonly intervalMs: number,
		private readonly timeoutMs: number,
	) {}

	start(): void {
		void this.run();
		this.timer = setInterval(() => void this.run(), this.intervalMs);
		this.timer.unref?.();
	}

	stop(): void {
		if (this.timer) clearInterval(this.timer);
	}

	healthy(): boolean {
		return this.ok;
	}

	async run(): Promise<void> {
		if (this.running) return;
		this.running = true;
		let timer: NodeJS.Timeout | undefined;
		try {
			await Promise.race([
				this.check(),
				new Promise<never>((_, reject) => {
					timer = setTimeout(() => reject(new Error("dependency check timed out")), this.timeoutMs);
				}),
			]);
			this.ok = true;
		} catch {
			this.ok = false;
		} finally {
			if (timer) clearTimeout(timer);
			this.running = false;
		}
	}
}

/**
 * P3.1 (MIG-02, AC4): readiness needs the applied schema at or above the
 * binary's minimum. Checked at start and every intervalMs until it holds,
 * so the replica becomes ready once the migration Job has run, without a
 * restart; the version pair is the readiness reason and is logged once per
 * change.
 */
export class SchemaGate {
	private reason: string | undefined = "schema not checked yet";
	private timer: NodeJS.Timeout | undefined;
	private inflight: Promise<void> | undefined;

	constructor(
		private readonly applied: () => Promise<number>,
		private readonly minimum: number,
		private readonly intervalMs: number,
		private readonly log: Logger,
	) {}

	start(): void {
		void this.run();
		this.timer = setInterval(() => void this.run(), this.intervalMs);
		this.timer.unref?.();
	}

	stop(): void {
		if (this.timer) clearInterval(this.timer);
	}

	satisfied(): boolean {
		return this.reason === undefined;
	}

	/** The reason readiness is withheld, or undefined. */
	why(): string | undefined {
		return this.reason === undefined ? undefined : `schema: ${this.reason}`;
	}

	/** One check; a call while one is in flight answers with that check. */
	run(): Promise<void> {
		if (this.reason === undefined) return Promise.resolve();
		this.inflight ??= this.check().finally(() => {
			this.inflight = undefined;
		});
		return this.inflight;
	}

	private async check(): Promise<void> {
		let next: string | undefined;
		try {
			const applied = await this.applied();
			next =
				applied < this.minimum ? `schema at version ${applied}, the binary needs at least ${this.minimum}` : undefined;
		} catch (err) {
			next = `schema version unreadable: ${err instanceof Error ? err.message : String(err)}`;
		}
		if (next !== undefined && next !== this.reason)
			this.log.error("schema check failed; the replica stays not ready", { error: next });
		this.reason = next;
		if (next === undefined) this.stop();
	}
}
