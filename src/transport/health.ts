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
