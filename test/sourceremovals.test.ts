// P2.7 (AC4): source deletions and ACL revocations survive a point-in-time
// restore, on real PostgreSQL 17. Each is answered only once its record is
// in the removal inventory; after a restore erased them, nothing is read,
// searched or frozen until a pass re-applied them under their original
// commands. The restore is simulated for these rows: the source, its ACL
// revisions, commands and removal rows return to their state at T while
// the inventory keeps its records.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { MemoryRemovalInventory } from "../src/adapters/removals.js";
import { Removals } from "../src/application/removals.js";
import { type IngestPlan, Sources } from "../src/application/sources.js";
import type { Tasks } from "../src/application/tasks.js";
import { type Command, type Scope, SourceError } from "../src/domain/source.js";
import { digestOf } from "../src/domain/task.js";
import { silentLogger } from "../src/log.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";

const clock = new FakeClock(new Date("2026-10-13T12:00:00Z"));
const objects = new MemoryObjects();
const inventory = new MemoryRemovalInventory();
let inst: Instance;
let tasks: Tasks;
let store: Store;
let sources: Sources;
let removals: Removals;
const plan: IngestPlan = {
	profileId: "parser-docling-dev-v1",
	profileRevision: 1,
	chunkerId: "docling-hierarchical",
	chunkerRevision: 1,
	mediaTypes: ["text/markdown"],
	maxInputBytes: 1 << 20,
};
const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const bob: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "bob" };
let seq = 0;
const cmd = (s: Scope): Command => ({
	tenantId: s.tenantId,
	commandId: `src-rm-${++seq}`,
	actorId: s.actorId,
	requestDigest: `sha256:${"2".repeat(64)}`,
});

async function code(p: Promise<unknown>): Promise<string> {
	try {
		await p;
		return "OK";
	} catch (err) {
		if (err instanceof SourceError) return err.code;
		throw err;
	}
}

beforeAll(async () => {
	inst = await startInstance();
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	sources = new Sources(
		store,
		tasks,
		() => objects,
		() => plan,
		() => ({ maxSourceBytes: 1 << 20, uploadTtlMs: 900_000, legacyUploads: true }),
		clock,
		silentLogger,
	);
}, 180_000);

afterAll(async () => {
	await store?.pool.end();
	await inst?.stop();
});

async function register(scope: Scope, name: string, text: string) {
	const bytes = Buffer.from(text);
	objects.objects.set(`uploads/${scope.tenantId}/${name}`, bytes);
	return (
		await sources.register(
			cmd(scope),
			scope,
			{
				kind: "document",
				locator: `upload:${name}`,
				contentDigest: digestOf(bytes),
				mediaType: "text/markdown",
				sizeBytes: String(bytes.length),
			},
			[{ principalType: "tenant", principalId: "tenant_a" }],
		)
	).source;
}

describe("source removals across a restore (P2.7 AC4)", () => {
	it("records each deletion and revocation before answering, closes reads after a restore and re-applies them", async () => {
		const { Registry } = await import("prom-client");
		const { Metrics } = await import("../src/metrics.js");
		const metrics = new Metrics(new Registry());
		const fresh = () =>
			new Removals(
				store,
				() => inventory,
				() => ({ windowMarginMs: 300_000, reconcileIntervalMs: 0 }),
				clock,
				silentLogger,
				metrics,
			);
		removals = fresh();
		removals.setTarget({ applyRestored: async () => "known" });
		removals.setSourceTarget(sources);
		sources.setRemovals(removals);
		await removals.pass(inventory);
		expect(removals.ready()).toBe(true);

		const s1 = await register(alice, "brief.md", "# Brief\n\nTeal.\n");
		const s2 = await register(alice, "notes.md", "# Notes\n\nSlate.\n");
		expect(await code(sources.get(bob, s1.sourceId))).toBe("OK");

		// After T: a revocation and a deletion, each recorded before its answer.
		const revoke = cmd(alice);
		inventory.failCreates.before = 1;
		expect(
			await code(
				sources.updateAccess(revoke, alice, s1.sourceId, 1, [{ principalType: "actor", principalId: "alice" }]),
			),
		).toBe("UNAVAILABLE");
		// The retried command completes its record.
		expect(
			(await sources.updateAccess(revoke, alice, s1.sourceId, 1, [{ principalType: "actor", principalId: "alice" }]))
				.existing,
		).toBe(true);
		const del = cmd(alice);
		await sources.delete(del, alice, s2.sourceId, 1);
		expect(inventory.records.size).toBe(2);
		const rows = await inst.admin("SELECT decision, state FROM source_removals ORDER BY decision");
		expect(rows.rows).toEqual([
			{ decision: "delete", state: "recorded" },
			{ decision: "revoke", state: "recorded" },
		]);
		expect(await code(sources.get(bob, s1.sourceId))).toBe("NOT_FOUND");

		// The restore to T: both decisions vanish from the database.
		for (const sql of [
			"DELETE FROM source_removals",
			`DELETE FROM source_commands WHERE command_id IN ('${revoke.commandId}', '${del.commandId}')`,
			"DELETE FROM source_acl WHERE acl_revision > 1",
			"DELETE FROM source_acl_revisions WHERE acl_revision > 1",
			"UPDATE sources SET acl_revision = 1, deleted = false, deleted_at = NULL",
		])
			await inst.admin(sql);
		expect(await code(sources.get(bob, s1.sourceId))).toBe("OK"); // what the restore revived

		// A new process on the restored database: sources are closed.
		removals = fresh();
		removals.setTarget({ applyRestored: async () => "known" });
		removals.setSourceTarget(sources);
		sources.setRemovals(removals);
		expect(removals.ready()).toBe(false);
		expect(await code(sources.get(bob, s1.sourceId))).toBe("UNAVAILABLE");
		expect(await code(sources.list(alice, "", 10))).toBe("UNAVAILABLE");

		const out = await removals.pass(inventory);
		expect(out.restored).toBe(2);
		expect(removals.ready()).toBe(true);
		expect(await code(sources.get(bob, s1.sourceId))).toBe("NOT_FOUND");
		expect((await sources.get(alice, s1.sourceId)).access).toEqual([{ principalType: "actor", principalId: "alice" }]);
		expect(await code(sources.get(alice, s2.sourceId))).toBe("NOT_FOUND");
		const restored = await inst.admin("SELECT state FROM source_removals");
		expect(restored.rows.every((r) => r.state === "restored")).toBe(true);
		expect((await removals.pass(inventory)).restored).toBe(0);
	});
});
