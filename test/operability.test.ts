// P2.7 (AC8): the process survives a database that pauses or drops it,
// readiness follows the database, and a hung object store ends at its bound.
import { createServer, type Server } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { S3Objects } from "../src/adapters/objects.js";
import { newPool } from "../src/adapters/postgres.js";
import { DependencyProbe } from "../src/transport/health.js";
import { type Instance, startInstance } from "./harness.js";

let inst: Instance;
beforeAll(async () => {
	inst = await startInstance();
}, 180_000);
afterAll(async () => {
	await inst?.stop();
});

describe("database bounds (P2.7)", () => {
	it("ends a statement at the bound and survives a terminated idle client", async () => {
		const errors: Error[] = [];
		const pool = newPool(
			inst.appUrl,
			2,
			{ connectTimeoutMs: 2_000, statementTimeoutMs: 500, idleTimeoutMs: 30_000, idleInTransactionTimeoutMs: 60_000 },
			(err) => errors.push(err),
		);
		try {
			await expect(pool.query("SELECT pg_sleep(3)")).rejects.toMatchObject({ code: "57014" });
			// An idle client the server terminates: reported, never an uncaught exception.
			const c = await pool.connect();
			const pid = (await c.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0]?.pid;
			c.release();
			await inst.root(`SELECT pg_terminate_backend(${Number(pid)})`);
			await expect.poll(() => errors.length, { timeout: 5_000 }).toBeGreaterThan(0);
			expect((await pool.query<{ one: number }>("SELECT 1 AS one")).rows[0]?.one).toBe(1);
		} finally {
			await pool.end();
		}
	});
});

describe("readiness probe (P2.7)", () => {
	it("is not ready before a success, drops on a failure or a hang, returns on the next success", async () => {
		let mode: "ok" | "fail" | "hang" = "ok";
		const probe = new DependencyProbe(
			() =>
				mode === "ok"
					? Promise.resolve()
					: mode === "fail"
						? Promise.reject(new Error("down"))
						: new Promise<void>(() => undefined),
			60_000,
			100,
		);
		expect(probe.healthy()).toBe(false);
		await probe.run();
		expect(probe.healthy()).toBe(true);
		mode = "fail";
		await probe.run();
		expect(probe.healthy()).toBe(false);
		mode = "ok";
		await probe.run();
		expect(probe.healthy()).toBe(true);
		mode = "hang";
		await probe.run();
		expect(probe.healthy()).toBe(false);
	});
});

describe("object store bound (P2.7)", () => {
	let hung: Server;
	let port = 0;
	beforeAll(async () => {
		// Accepts the connection and never answers.
		hung = createServer(() => undefined);
		await new Promise<void>((r) => hung.listen(0, "127.0.0.1", () => r()));
		port = (hung.address() as { port: number }).port;
	});
	afterAll(() => hung.close());

	it("a hung store read ends at the configured timeout", async () => {
		const store = new S3Objects({
			endpoint: `http://127.0.0.1:${port}`,
			stageEndpoint: "",
			bucket: "anvilkit-knowledge",
			region: "us-east-1",
			accessKeyId: "x",
			secretAccessKey: "y",
			requestTimeoutMs: 1_000,
		});
		const began = Date.now();
		await expect(store.read("uploads/t/x", 10)).rejects.toBeDefined();
		expect(Date.now() - began).toBeLessThan(5_000);
		store.close();
	}, 15_000);
});
