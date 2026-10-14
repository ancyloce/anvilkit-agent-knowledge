// The plaintext probe and metrics listener: /healthz answers 200 while the
// process runs, /readyz 200 only while the gRPC listener serves and no
// shutdown has started, /metrics the Prometheus exposition. First up, last
// down, so a scrape during the drain still sees the shutdown gauges.
import { createServer, type Server } from "node:http";
import type { Registry } from "prom-client";

export function createHealthServer(registry: Registry, isReady: () => boolean): Server {
	return createServer((req, res) => {
		if (req.url === "/healthz") {
			res.writeHead(200).end();
			return;
		}
		if (req.url === "/readyz") {
			res.writeHead(isReady() ? 200 : 503).end();
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
