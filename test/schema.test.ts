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
	// The migration Job applies every migration it carries; the parent's set
	// may be ahead of the build's minimum (00013 revokes bookkeeping writes,
	// which no statement depends on).
	it("the harness reaches at least the build's minimum, as the migration Job does", async () => {
		const pool = new pg.Pool({ connectionString: inst.appUrl, max: 1 });
		try {
			expect(await schemaVersion(pool)).toBeGreaterThanOrEqual(minimumSchemaVersion);
		} finally {
			await pool.end();
		}
	});

	it("withholds readiness behind the minimum, names both versions and recovers without a restart", async () => {
		// Behind the minimum: the minimum's row and every row after it are taken
		// away, and given back afterwards.
		const removed = (
			await inst.admin("DELETE FROM goose_db_version WHERE version_id >= $1 RETURNING version_id", [
				minimumSchemaVersion,
			])
		).rows.map((r) => Number(r.version_id));
		const ahead = Math.max(minimumSchemaVersion + 1, ...removed.map((v) => v + 1));
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
			// Ahead of the minimum (the next release's expand migration) is accepted.
			await inst.admin("INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)", [ahead]);
			const aheadGate = new SchemaGate(() => schemaVersion(pool), minimumSchemaVersion, 20, log);
			await aheadGate.run();
			expect(aheadGate.satisfied()).toBe(true);
		} finally {
			gate.stop();
			server.close();
			await pool.end();
			await inst.admin("DELETE FROM goose_db_version WHERE version_id = $1", [ahead]);
			for (const v of removed) {
				await inst.admin(
					"INSERT INTO goose_db_version (version_id, is_applied) SELECT $1, true WHERE NOT EXISTS (SELECT 1 FROM goose_db_version WHERE version_id = $1)",
					[v],
				);
			}
		}
	});
});
