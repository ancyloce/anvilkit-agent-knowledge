// The parser launcher's Kubernetes access (DD-07 §2, platform.md
// anvilkit-parsing): Jobs, their Pods and the per-launch stage Secret in one
// namespace, through the least Role the Knowledge chart grants. Every call
// is exactly one HTTPS request: no client retries, no redirects followed, so
// a lost answer is resolved by reading the deterministic name again, never
// by a blind second create. Deletion is bound to the observed UID.
import { readFileSync } from "node:fs";
import https from "node:https";
import { parse as parseYaml } from "yaml";

export interface KubeConfig {
	server: string;
	ca: Buffer | undefined;
	token: () => string;
}

export class KubeError extends Error {
	constructor(
		readonly status: number,
		readonly reason: string,
		message: string,
	) {
		super(message);
	}
}

/** A kubeconfig file (server, CA data, token) or the Pod's ServiceAccount files. */
export function loadKubeConfig(kubeconfigPath: string): KubeConfig {
	if (kubeconfigPath) {
		const doc = parseYaml(readFileSync(kubeconfigPath, "utf8")) as {
			clusters?: { cluster?: { server?: string; "certificate-authority-data"?: string } }[];
			users?: { user?: { token?: string } }[];
		};
		const cluster = doc.clusters?.[0]?.cluster;
		const token = doc.users?.[0]?.user?.token;
		if (!cluster?.server || !token) throw new Error("kubeconfig: server and token are required");
		const caData = cluster["certificate-authority-data"];
		return { server: cluster.server, ca: caData ? Buffer.from(caData, "base64") : undefined, token: () => token };
	}
	const sa = "/var/run/secrets/kubernetes.io/serviceaccount";
	const host = process.env.KUBERNETES_SERVICE_HOST;
	const port = process.env.KUBERNETES_SERVICE_PORT ?? "443";
	if (!host) throw new Error("no kubeconfig and not in a cluster");
	return {
		server: `https://${host.includes(":") ? `[${host}]` : host}:${port}`,
		ca: readFileSync(`${sa}/ca.crt`),
		// Projected tokens rotate: read at every request.
		token: () => readFileSync(`${sa}/token`, "utf8").trim(),
	};
}

export interface Kube {
	create(path: string, body: unknown): Promise<Record<string, unknown>>;
	get(path: string): Promise<Record<string, unknown> | undefined>;
	delete(path: string, uid: string): Promise<"deleted" | "gone">;
	close(): void;
}

export class KubeClient implements Kube {
	private readonly agent: https.Agent;

	constructor(
		private readonly cfg: KubeConfig,
		private readonly timeoutMs: number,
	) {
		this.agent = new https.Agent({ ca: cfg.ca, keepAlive: true, maxSockets: 8 });
	}

	private request(method: string, path: string, body?: unknown): Promise<{ status: number; json: unknown }> {
		const url = new URL(path, this.cfg.server);
		const data = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
		return new Promise((resolve, reject) => {
			const req = https.request(
				url,
				{
					method,
					agent: this.agent,
					timeout: this.timeoutMs,
					headers: {
						Authorization: `Bearer ${this.cfg.token()}`,
						Accept: "application/json",
						...(data ? { "Content-Type": "application/json", "Content-Length": data.length } : {}),
					},
				},
				(res) => {
					const parts: Buffer[] = [];
					let size = 0;
					res.on("data", (c: Buffer) => {
						size += c.length;
						if (size > 4 << 20) req.destroy(new Error("response over 4 MiB"));
						else parts.push(c);
					});
					res.on("end", () => {
						let json: unknown;
						try {
							json = parts.length ? JSON.parse(Buffer.concat(parts).toString("utf8")) : undefined;
						} catch {
							json = undefined;
						}
						resolve({ status: res.statusCode ?? 0, json });
					});
					res.on("error", reject);
				},
			);
			req.on("timeout", () => req.destroy(new Error("kubernetes request timed out")));
			req.on("error", reject);
			req.end(data);
		});
	}

	private static fail(status: number, json: unknown): KubeError {
		const s = (json ?? {}) as { reason?: string; message?: string };
		// The API server's own explanation (field paths, admission policy
		// messages); the objects this launcher sends carry no secret.
		return new KubeError(
			status,
			s.reason ?? "",
			`${status} ${s.reason ?? ""} ${(s.message ?? "").slice(0, 600)}`.trim(),
		);
	}

	async create(path: string, body: unknown): Promise<Record<string, unknown>> {
		const r = await this.request("POST", path, body);
		if (r.status === 201 || r.status === 200) return r.json as Record<string, unknown>;
		throw KubeClient.fail(r.status, r.json);
	}

	async get(path: string): Promise<Record<string, unknown> | undefined> {
		const r = await this.request("GET", path);
		if (r.status === 404) return undefined;
		if (r.status === 200) return r.json as Record<string, unknown>;
		throw KubeClient.fail(r.status, r.json);
	}

	async delete(path: string, uid: string): Promise<"deleted" | "gone"> {
		const r = await this.request("DELETE", path, {
			kind: "DeleteOptions",
			apiVersion: "v1",
			propagationPolicy: "Foreground",
			preconditions: { uid },
		});
		if (r.status === 200 || r.status === 202) return "deleted";
		// Gone, or a different object now holds the name: nothing of ours to delete.
		if (r.status === 404 || r.status === 409) return "gone";
		throw KubeClient.fail(r.status, r.json);
	}

	close(): void {
		this.agent.destroy();
	}
}
