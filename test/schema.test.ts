// P3.1 (MIG-02, AC4): a replica whose database is behind the build's
// minimum schema stays not ready with the version pair as the readiness
// reason and in the log; it becomes ready once the version is reached,
// without a restart; a schema ahead of the minimum is accepted.
import type { AddressInfo } from "node:net";
import pg from "pg";
import { Registry } from "prom-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { minimumSchemaVersion, schemaVersion } from "../src/adapters/postgres.js";
import type { Logger } from "../src/log.js";
import { createHealthServer, listen, SchemaGate } from "../src/transport/health.js";
import { type Instance, startInstance } from "./harness.js";

let inst: Instance;
beforeAll(async () => {
	inst = await startInstance();
}, 180_000);
afterAll(async () => {
	await inst?.stop();
});

describe("schema version gate (P3.1)", () => {
	it("the harness reaches the build's minimum exactly as the migration Job does", async () => {
		const pool = new pg.Pool({ connectionString: inst.appUrl, max: 1 });
		try {
			expect(await schemaVersion(pool)).toBe(minimumSchemaVersion);
		} finally {
			await pool.end();
		}
	});

	it("withholds readiness behind the minimum, names both versions and recovers without a restart", async () => {
		await inst.admin("DELETE FROM goose_db_version WHERE version_id = $1", [minimumSchemaVersion]);
		const pool = new pg.Pool({ connectionString: inst.appUrl, max: 1 });
		const errors: string[] = [];
		const log: Logger = { info: () => {}, warn: () => {}, error: (m, f) => errors.push(`${m} ${String(f?.error)}`) };
		const gate = new SchemaGate(() => schemaVersion(pool), minimumSchemaVersion, 20, log);
		const server = createHealthServer(
			new Registry(),
			() => gate.satisfied(),
			() => gate.why(),
		);
		await listen(server, "127.0.0.1:0");
		const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/readyz`;
		try {
			gate.start();
			const pair = `schema at version ${minimumSchemaVersion - 1}, the binary needs at least ${minimumSchemaVersion}`;
			await expect
				.poll(async () => {
					const r = await fetch(url);
					return `${r.status} ${await r.text()}`;
				})
				.toBe(`503 schema: ${pair}`);
			expect(errors).toEqual([`schema check failed; the replica stays not ready ${pair}`]);
			await inst.admin("INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)", [
				minimumSchemaVersion,
			]);
			await expect.poll(async () => (await fetch(url)).status).toBe(200);
			// One ahead (the next release's expand migration) is accepted.
			await inst.admin("INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)", [
				minimumSchemaVersion + 1,
			]);
			const ahead = new SchemaGate(() => schemaVersion(pool), minimumSchemaVersion, 20, log);
			await ahead.run();
			expect(ahead.satisfied()).toBe(true);
		} finally {
			gate.stop();
			server.close();
			await pool.end();
			await inst.admin("DELETE FROM goose_db_version WHERE version_id = $1", [minimumSchemaVersion + 1]);
		}
	});
});
