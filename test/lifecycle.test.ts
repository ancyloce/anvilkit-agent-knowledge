// The assembled service on a real database: gRPC health and validation,
// secret rotation with the pool replaced and drained, a rejected candidate
// leaving the active generation, the ordered shutdown, and a failed start
// that unwinds its listeners.
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import {
	BackgroundTaskServiceClient,
	ClaimTaskRequest,
	GetTaskRequest,
} from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { credentials, type ServiceError, status } from "@grpc/grpc-js";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadFrom } from "../src/config.js";
import { silentLogger } from "../src/log.js";
import { type Started, start } from "../src/main.js";
import { type Instance, startInstance } from "./harness.js";

let inst: Instance;

beforeAll(async () => {
	inst = await startInstance();
});

beforeEach(async () => {
	await inst.root("ALTER ROLE anvilkit_knowledge_app PASSWORD 'app'");
});

afterAll(async () => {
	await inst.stop();
});

function freePort(): Promise<number> {
	return new Promise((resolve) => {
		const s = createServer();
		s.listen(0, "127.0.0.1", () => {
			const port = (s.address() as { port: number }).port;
			s.close(() => resolve(port));
		});
	});
}

async function metric(health: string, name: string): Promise<string> {
	const res = await fetch(`http://${health}/metrics`);
	const body = await res.text();
	const line = body.split("\n").find((l) => l.startsWith(`${name} `));
	return line ? line.slice(name.length + 1).trim() : "";
}

async function readyStatus(health: string): Promise<number> {
	try {
		return (await fetch(`http://${health}/readyz`)).status;
	} catch {
		return 0;
	}
}

function call<T>(fn: (cb: (err: ServiceError | null, res: T) => void) => void): Promise<T> {
	return new Promise((resolve, reject) => fn((err, res) => (err ? reject(err) : resolve(res))));
}

async function until(fn: () => Promise<boolean>, timeoutMs: number): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await fn()) return;
		await new Promise((r) => setTimeout(r, 100));
	}
	throw new Error("condition never satisfied");
}

describe("lifecycle", () => {
	it("serves, rotates the secret with a probed pool swap, rejects a broken candidate and stops in order", async () => {
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-"));
		const secret = path.join(dir, "database-url");
		writeFileSync(secret, inst.appUrl);
		const cfgFile = path.join(dir, "config.yaml");
		writeFileSync(cfgFile, "reload:\n  interval: 200ms\n  drain_limit: 5s\ntasks:\n  sweep_interval: 200ms\n");
		const grpcAddr = `127.0.0.1:${await freePort()}`;
		const healthAddr = `127.0.0.1:${await freePort()}`;
		const environ = {
			ANVILKIT_KNOWLEDGE_CONFIG: cfgFile,
			ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE: secret,
			ANVILKIT_KNOWLEDGE_LISTEN: grpcAddr,
			ANVILKIT_KNOWLEDGE_HEALTH_LISTEN: healthAddr,
		};
		const started: Started = await start(loadFrom(cfgFile, environ, 1), silentLogger, environ);
		expect(await readyStatus(healthAddr)).toBe(200);
		expect(await metric(healthAddr, "anvilkit_knowledge_config_generation")).toBe("1");
		const client = new BackgroundTaskServiceClient(grpcAddr, credentials.createInsecure());
		await expect(
			call((cb) => client.getTask(GetTaskRequest.fromPartial({ taskId: "missing" }), cb)),
		).rejects.toMatchObject({ code: status.NOT_FOUND });
		await expect(
			call((cb) =>
				client.claimTask(
					ClaimTaskRequest.fromPartial({ taskId: "x", generation: "1", workerId: "w", leaseSeconds: 0 }),
					cb,
				),
			),
		).rejects.toMatchObject({ code: status.INVALID_ARGUMENT });

		// Secret rotation: the role's password changes, then the mounted file;
		// calls after the swap prove the new generation's pool serves.
		await inst.root("ALTER ROLE anvilkit_knowledge_app PASSWORD 'rotated'");
		const rotated = inst.appUrl.replace(":app@", ":rotated@");
		writeFileSync(secret, rotated);
		await until(async () => (await metric(healthAddr, "anvilkit_knowledge_config_generation")) === "2", 10_000);
		expect(await metric(healthAddr, "anvilkit_knowledge_config_rotations_total")).toBe("1");
		for (let i = 0; i < 5; i++)
			await expect(
				call((cb) => client.getTask(GetTaskRequest.fromPartial({ taskId: `missing-${i}` }), cb)),
			).rejects.toMatchObject({ code: status.NOT_FOUND });

		// A candidate the database refuses fails its probe and is rejected; generation 2 stays.
		writeFileSync(secret, inst.appUrl.replace(":app@", ":wrong@"));
		await until(async () => {
			const v = await metric(healthAddr, "anvilkit_knowledge_config_rejections_total");
			return v !== "" && v !== "0";
		}, 20_000);
		expect(await metric(healthAddr, "anvilkit_knowledge_config_generation")).toBe("2");
		await expect(
			call((cb) => client.getTask(GetTaskRequest.fromPartial({ taskId: "still" }), cb)),
		).rejects.toMatchObject({ code: status.NOT_FOUND });
		writeFileSync(secret, rotated);

		await started.stop();
		expect(await readyStatus(healthAddr)).toBe(0);
		client.close();
	});

	it("unwinds a failed start", async () => {
		const blocker = createServer();
		await new Promise<void>((r) => blocker.listen(0, "127.0.0.1", () => r()));
		const blocked = `127.0.0.1:${(blocker.address() as { port: number }).port}`;
		const healthAddr = `127.0.0.1:${await freePort()}`;
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-"));
		const cfgFile = path.join(dir, "config.yaml");
		writeFileSync(cfgFile, "{}\n");
		const environ = {
			ANVILKIT_KNOWLEDGE_CONFIG: cfgFile,
			ANVILKIT_KNOWLEDGE_DATABASE_URL: inst.appUrl,
			ANVILKIT_KNOWLEDGE_LISTEN: blocked,
			ANVILKIT_KNOWLEDGE_HEALTH_LISTEN: healthAddr,
		};
		await expect(start(loadFrom(cfgFile, environ, 1), silentLogger, environ)).rejects.toThrow();
		expect(await readyStatus(healthAddr)).toBe(0);
		blocker.close();
	});
});

it("F08 expires the active snapshot, preserves it after a rejected candidate and renews unchanged values", async () => {
	const dir = mkdtempSync(path.join(tmpdir(), "knowledge-expiry-"));
	const cfgFile = path.join(dir, "config.yaml"),
		snapshot = path.join(dir, "snapshot.json");
	writeFileSync(cfgFile, "apollo:\n  mode: snapshot\nreload:\n  interval: 100ms\n  drain_limit: 2s\n");
	const healthAddr = `127.0.0.1:${await freePort()}`,
		grpcAddr = `127.0.0.1:${await freePort()}`;
	const doc = {
		schemaVersion: 1,
		appId: "anvilkit-agent-knowledge",
		cluster: "default",
		namespace: "application",
		releaseKey: "20260918120000-0123456789ab",
		fetchedAt: new Date(Date.now() - 1000).toISOString(),
		expiresAt: new Date(Date.now() + 3000).toISOString(),
		configurations: {},
	};
	writeFileSync(snapshot, JSON.stringify(doc));
	const env = {
		ANVILKIT_KNOWLEDGE_CONFIG: cfgFile,
		ANVILKIT_KNOWLEDGE_DATABASE_URL: inst.appUrl,
		ANVILKIT_KNOWLEDGE_APOLLO_SNAPSHOT_FILE: snapshot,
		ANVILKIT_KNOWLEDGE_LISTEN: grpcAddr,
		ANVILKIT_KNOWLEDGE_HEALTH_LISTEN: healthAddr,
	};
	const running = await start(loadFrom(cfgFile, env, 1), silentLogger, env);
	const client = new BackgroundTaskServiceClient(grpcAddr, credentials.createInsecure());
	try {
		writeFileSync(snapshot, "invalid");
		await until(async () => Number(await metric(healthAddr, "anvilkit_knowledge_config_rejections_total")) > 0, 2000);
		expect(await readyStatus(healthAddr)).toBe(200);
		await until(async () => (await readyStatus(healthAddr)) === 503, 5000);
		await expect(
			call((cb) =>
				client.claimTask(
					ClaimTaskRequest.fromPartial({ taskId: "expired", generation: "1", workerId: "worker", leaseSeconds: 10 }),
					cb,
				),
			),
		).rejects.toMatchObject({ code: status.FAILED_PRECONDITION });
		doc.expiresAt = new Date(Date.now() + 60000).toISOString();
		writeFileSync(snapshot, JSON.stringify(doc));
		await until(async () => (await readyStatus(healthAddr)) === 200, 5000);
	} finally {
		client.close();
		await running.stop();
	}
});
