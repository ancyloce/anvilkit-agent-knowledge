// P23: memory deletions and revocations survive a point-in-time restore of
// anvilkit_knowledge, on real PostgreSQL 17 (with the Store's vendor
// schema) and Qdrant 1.19.0. Each removal is answered only once its record
// is in the removal inventory; a record whose write is not confirmed is
// completed by a retry of the command or by the next pass. The restore is
// simulated faithfully for this purpose: every table of the database (the
// facts, decisions, removal rows, projection ledger, requests, outbox and
// the PostgresStore's schema) returns to its state at T while Qdrant and
// the inventory keep theirs. Until a pass has listed the window completely
// nothing is read, decided, recalled or projected; the pass re-applies each
// erased removal under its original command identity, and a fact the
// restore erased altogether cannot be proposed again by a retried command.
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as mdb from "../src/adapters/memorydb.js";
import * as db from "../src/adapters/postgres.js";
import { MemoryRemovalInventory, RemovalListIncomplete } from "../src/adapters/removals.js";
import { Recall, type RecallRequest } from "../src/application/recall.js";
import { Removals } from "../src/application/removals.js";
import { collectionOf } from "../src/domain/index.js";
import { type Fact, MemoryError, memoryPointIdOf, projectionTaskIdOf } from "../src/domain/memory.js";
import {
	parseRemoval,
	RemovalError,
	type RemovalRecord,
	removalBodyOf,
	removalKeyOf,
	restoredRemoval,
} from "../src/domain/removal.js";
import type { Command, Scope } from "../src/domain/source.js";
import { silentLogger } from "../src/log.js";
import { acceptGeneration, drainProjections, type Kit, startKit } from "./memorykit.js";

let k: Kit;
let su: pg.Client;
let recall: Recall;
let removals: Removals;
let generation = 0;
const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const agent: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "agent-coder" };
let n = 0;

async function propose(content: string, cmd: Command = k.cmd(agent)): Promise<Fact> {
	return (
		await k.memory.propose(cmd, agent, {
			subjectType: "actor",
			subjectId: "alice",
			content,
			sourceRefs: [],
			expiresAt: null,
			origin: "model",
		})
	).fact;
}

async function confirmed(content = `alice keeps a changelog ${++n}`): Promise<Fact> {
	const p = await propose(content);
	return (await k.memory.decide(k.cmd(alice), alice, p.factId, 1, "confirm", "", null)).fact;
}

async function code(p: Promise<unknown>): Promise<string> {
	try {
		await p;
		return "OK";
	} catch (err) {
		if (err instanceof MemoryError) return err.code;
		throw err;
	}
}

const ask = (query: string, extra: Partial<RecallRequest> = {}) =>
	recall.recall({
		scope: alice,
		query,
		maxFacts: 10,
		retrievalProfileId: "hybrid-bge-m3-dev-v1",
		deadline: new Date(k.clock.now().getTime() + 30_000),
		...extra,
	});

async function row(tenantId: string, commandId: string) {
	return mdb.removalByCommand(k.store.pool, tenantId, commandId);
}

async function point(factId: string) {
	return (await k.vectors.retrieve(collectionOf(generation), [memoryPointIdOf(factId)])).get(memoryPointIdOf(factId));
}

// ---------------------------------------------------------------------------
// The simulated restore: a superuser copy of every table of both schemas.
// ---------------------------------------------------------------------------

let snaps = 0;

async function tables(): Promise<{ schema: string; table: string; columns: string }[]> {
	const r = await su.query<{ schema: string; table: string; columns: string }>(
		`SELECT n.nspname AS schema, c.relname AS table,
		   string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) AS columns
		 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
		 WHERE c.relkind = 'r' AND n.nspname IN ('public', 'memory_store')
		 GROUP BY n.nspname, c.relname ORDER BY 1, 2`,
	);
	return r.rows;
}

async function snapshot(): Promise<string> {
	const name = `pitr_${++snaps}`;
	await su.query(`CREATE SCHEMA ${name}`);
	for (const t of await tables())
		await su.query(`CREATE TABLE ${name}."${t.schema}__${t.table}" AS TABLE "${t.schema}"."${t.table}"`);
	return name;
}

async function restore(name: string): Promise<void> {
	const all = await tables();
	await su.query("BEGIN");
	try {
		await su.query("SET LOCAL session_replication_role = replica");
		await su.query(`TRUNCATE ${all.map((t) => `"${t.schema}"."${t.table}"`).join(", ")}`);
		for (const t of all)
			await su.query(
				`INSERT INTO "${t.schema}"."${t.table}" (${t.columns}) OVERRIDING SYSTEM VALUE
				 SELECT ${t.columns} FROM ${name}."${t.schema}__${t.table}"`,
			);
		await su.query("COMMIT");
	} catch (err) {
		await su.query("ROLLBACK");
		throw err;
	}
}

/** A new process on the restored database: memory closed until its first pass. */
function restart(): Removals {
	removals = new Removals(
		k.store,
		() => k.inventory,
		() => k.removalBounds,
		k.clock,
		silentLogger,
		k.metrics,
	);
	k.memory.setRemovals(removals);
	return removals;
}

beforeAll(async () => {
	k = await startKit();
	await acceptGeneration(k);
	generation = Number(
		(await k.inst.admin("SELECT max(generation)::int AS g FROM index_generations WHERE state = 'accepted'")).rows[0]?.g,
	);
	su = new pg.Client({ connectionString: k.inst.adminUrl.replace(/\/postgres$/, "/anvilkit_knowledge") });
	await su.connect();
	removals = k.removals;
	k.projector.setGate(() => removals.ready());
	recall = new Recall(
		k.store,
		() => k.vectors,
		() => k.inference,
		() => k.activeSpace.current,
		() => ({
			profileId: "hybrid-bge-m3-dev-v1",
			denseLimit: 20,
			sparseLimit: 20,
			rrfK: 60,
			fusedLimit: 20,
			rerankLimit: 10,
			maxContextChars: 4000,
			maxAllowedSources: 8,
			minRerankScore: 0,
			maxDeadlineMs: 60_000,
		}),
		() => k.bounds,
		k.clock,
		silentLogger,
		k.metrics,
	);
	recall.setGate(() => removals.ready());
}, 240_000);

afterAll(async () => {
	await su?.end();
	await k?.stop();
});

describe("removal records", () => {
	it("refuses a record not bound to its key, not canonical or malformed; restores the most restrictive state", () => {
		const r: RemovalRecord = {
			schemaVersion: 1,
			kind: "memory-removal",
			tenantId: "tenant_a",
			commandId: "cmd-1",
			requestDigest: `sha256:${"a".repeat(64)}`,
			factId: `mem-${"0".repeat(32)}`,
			decision: "revoke",
			decider: "alice",
			confirmer: "bob",
			reasonCode: "OUTDATED",
			fromRevision: 2,
			toRevision: 3,
			recordedAt: "2026-10-02T15:01:12.123Z",
		};
		const scope = "0f8e5c2a-6d1b-4c3e-9a7f-2b4d6e8f0a1c";
		const key = removalKeyOf(scope, r);
		expect(key).toMatch(/^removals\/0f8e5c2a-6d1b-4c3e-9a7f-2b4d6e8f0a1c\/20261002T150112\.123Z\/[0-9a-f]{32}\.json$/);
		expect(parseRemoval(scope, key, removalBodyOf(r))).toEqual(r);
		const other = removalKeyOf(scope, { ...r, commandId: "cmd-2" });
		expect(() => parseRemoval(scope, other, removalBodyOf(r))).toThrow(RemovalError);
		// Another database's scope never binds this database's record.
		expect(() => parseRemoval("1f8e5c2a-6d1b-4c3e-9a7f-2b4d6e8f0a1c", key, removalBodyOf(r))).toThrow(RemovalError);
		expect(() => removalKeyOf("../other", r)).toThrow(RemovalError);
		const spaced = Buffer.from(JSON.stringify(JSON.parse(removalBodyOf(r).toString()), null, 1));
		expect(() => parseRemoval(scope, key, spaced)).toThrow(RemovalError);
		for (const bad of [
			{ ...r, confirmer: "" },
			{ ...r, toRevision: 4 },
			{ ...r, decision: "reject" },
			{ ...r, extra: 1 },
		]) {
			const body = Buffer.from(JSON.stringify(bad));
			expect(() => parseRemoval(scope, key, body)).toThrow(RemovalError);
		}
		const base = { state: "confirmed", deleted: false, revision: 2, confirmer: "bob", content: "x" } as Fact;
		expect(restoredRemoval(base, r)).toMatchObject({ state: "revoked", revision: 3 });
		expect(restoredRemoval({ ...base, state: "proposed", confirmer: "" }, r)).toMatchObject({
			state: "revoked",
			confirmer: "bob",
		});
		expect(restoredRemoval({ ...base, state: "rejected" }, r)).toBeUndefined();
		expect(restoredRemoval({ ...base, deleted: true }, r)).toBeUndefined();
		const del = { ...r, decision: "delete" as const, confirmer: "", reasonCode: "" };
		expect(restoredRemoval({ ...base, state: "revoked" }, del)).toMatchObject({
			deleted: true,
			content: "",
			revision: 3,
		});
	});

	it("answers a deletion and a revocation only once their records are in the inventory; no content is recorded", async () => {
		const f = await confirmed("alice drafts in the evening");
		const g = await confirmed("alice reviews on mondays");
		const del = k.cmd(alice);
		const rev = k.cmd(alice);
		await k.memory.delete(del, alice, f.factId, 2);
		await k.memory.decide(rev, alice, g.factId, 2, "revoke", "OUTDATED", null);
		for (const [cmd, decision] of [
			[del, "delete"],
			[rev, "revoke"],
		] as const) {
			const x = await row(cmd.tenantId, cmd.commandId);
			expect(x?.state).toBe("recorded");
			const body = k.inventory.records.get(x?.inventoryKey ?? "");
			expect(body).toBeDefined();
			const rec = parseRemoval(await mdb.removalScope(k.store.pool), x?.inventoryKey ?? "", body as Buffer);
			expect(rec).toMatchObject({
				decision,
				decider: "alice",
				commandId: cmd.commandId,
				fromRevision: 2,
				toRevision: 3,
			});
			expect(body?.toString()).not.toContain("alice drafts");
			expect(body?.toString()).not.toContain("alice reviews");
		}
		const revoked = await row(rev.tenantId, rev.commandId);
		expect(revoked?.record).toMatchObject({ confirmer: "alice", reasonCode: "OUTDATED" });
	});

	it("answers UNAVAILABLE while the record is unconfirmed; the decision stays committed and a retry records it", async () => {
		// The write is lost: nothing reaches the inventory.
		const f = await confirmed();
		const del = k.cmd(alice);
		k.inventory.failCreates.before = 1;
		expect(await code(k.memory.delete(del, alice, f.factId, 2))).toBe("UNAVAILABLE");
		expect(await code(k.memory.get(alice, f.factId))).toBe("NOT_FOUND");
		expect((await row(del.tenantId, del.commandId))?.state).toBe("pending");
		const again = await k.memory.delete(del, alice, f.factId, 2);
		expect(again.existing).toBe(true);
		const x = await row(del.tenantId, del.commandId);
		expect(x?.state).toBe("recorded");
		expect(k.inventory.records.has(x?.inventoryKey ?? "")).toBe(true);
		// The answer is lost: the record was written; the retry confirms the same bytes.
		const g = await confirmed();
		const rev = k.cmd(alice);
		k.inventory.failCreates.after = 1;
		const before = k.inventory.records.size;
		expect(await code(k.memory.decide(rev, alice, g.factId, 2, "revoke", "", null))).toBe("UNAVAILABLE");
		expect((await k.memory.decide(rev, alice, g.factId, 2, "revoke", "", null)).existing).toBe(true);
		expect(k.inventory.records.size).toBe(before + 1);
		expect((await row(rev.tenantId, rev.commandId))?.state).toBe("recorded");
	});

	it("a pass records what a command left pending and what was removed while no inventory was placed", async () => {
		const f = await confirmed();
		const del = k.cmd(alice);
		k.inventory.failCreates.before = 1;
		expect(await code(k.memory.delete(del, alice, f.factId, 2))).toBe("UNAVAILABLE");
		k.setInventory(undefined);
		const g = await confirmed();
		const rev = k.cmd(alice);
		// Unplaced: answered, unprotected, pending.
		await k.memory.decide(rev, alice, g.factId, 2, "revoke", "", null);
		expect((await row(rev.tenantId, rev.commandId))?.state).toBe("pending");
		k.setInventory(k.inventory);
		k.removalBounds.pendingGraceMs = 0;
		try {
			const out = await removals.pass(k.inventory);
			expect(out.recorded).toBeGreaterThanOrEqual(2);
		} finally {
			k.removalBounds.pendingGraceMs = undefined;
		}
		for (const cmd of [del, rev]) {
			const x = await row(cmd.tenantId, cmd.commandId);
			expect(x?.state).toBe("recorded");
			expect(k.inventory.records.has(x?.inventoryKey ?? "")).toBe(true);
		}
	});
});

describe("point-in-time restore of the database", () => {
	it("re-applies every removal the restore erased before memory reads, decides, recalls or projects again", async () => {
		const a = await confirmed("alice prefers tabs in the restore drill");
		const b = await confirmed("alice prefers short commits in the restore drill");
		const c = await confirmed("alice prefers dark slides in the restore drill");
		const g = await confirmed("alice prefers green builds in the restore drill");
		const dProp = k.cmd(agent);
		const d = await propose("alice prefers rebases in the restore drill", dProp);
		await k.memory.delete(k.cmd(alice), alice, c.factId, 2);
		await drainProjections(k);
		expect((await ask("restore drill")).facts.map((f) => f.factId)).toEqual(
			expect.arrayContaining([a.factId, b.factId, g.factId]),
		);

		const t = await snapshot();

		// After T: four removals the restore will erase.
		const delA = k.cmd(alice);
		await k.memory.delete(delA, alice, a.factId, 2);
		const revB = k.cmd(alice);
		await k.memory.decide(revB, alice, b.factId, 2, "revoke", "OUTDATED", null);
		const confD = k.cmd(alice);
		await k.memory.decide(confD, alice, d.factId, 1, "confirm", "", null);
		const revD = k.cmd(alice);
		await k.memory.decide(revD, alice, d.factId, 2, "revoke", "", null);
		const eProp = k.cmd(agent);
		const e = await propose("alice prefers spaces in the restore drill", eProp);
		await k.memory.decide(k.cmd(alice), alice, e.factId, 1, "confirm", "", null);
		await drainProjections(k);
		const delE = k.cmd(alice);
		await k.memory.delete(delE, alice, e.factId, 2);
		await drainProjections(k);
		expect(await point(a.factId)).toBeUndefined();

		await restore(t);

		// The restore revived the removed content in the database and in the Store it holds.
		const revived = await k.inst.admin("SELECT state, deleted, content FROM memory_facts WHERE fact_id = $1", [
			a.factId,
		]);
		expect(revived.rows[0]).toMatchObject({ state: "confirmed", deleted: false, content: a.content });
		expect((await k.memoryStore.get("tenant_a", a.factId))?.content).toBe(a.content);
		expect(await mdb.getFact(k.store.pool, e.factId)).toBeUndefined();

		// A new process on the restored database: memory is closed.
		restart();
		expect(removals.ready()).toBe(false);
		expect(await code(k.memory.get(alice, a.factId))).toBe("UNAVAILABLE");
		expect(await code(k.memory.list(alice, { subjectType: "", subjectId: "", state: "" }, "", 10))).toBe("UNAVAILABLE");
		expect(await code(ask("restore drill"))).toBe("UNAVAILABLE");
		expect(await code(k.memory.delete(k.cmd(alice), alice, b.factId, 2))).toBe("UNAVAILABLE");
		// A rebuild (memoryctl) requests the restored confirmed fact; nothing is written while closed.
		await k.projector.rebuild(0);
		const taskId = projectionTaskIdOf(a.factId, 0);
		const latest = await db.getLatestRequest(k.store.pool, taskId);
		const claimed = await k.tasks.claim(taskId, latest?.generation ?? 0, "restore-worker", 600_000);
		expect(
			await k.projector.advance(taskId, latest?.generation ?? 0, "restore-worker", claimed.task.inputDigest),
		).toMatchObject({ state: "running" });

		// An incomplete listing keeps it closed.
		k.inventory.failListAfter = 0;
		await expect(removals.tick()).rejects.toBeInstanceOf(RemovalListIncomplete);
		expect(removals.ready()).toBe(false);
		k.inventory.resetFaults();

		const out = await removals.tick();
		expect(out?.restored).toBe(4);
		expect(removals.ready()).toBe(true);

		// a: deleted again, under its original command; its retry replays.
		expect(await code(k.memory.get(alice, a.factId))).toBe("NOT_FOUND");
		const fa = await mdb.getFact(k.store.pool, a.factId);
		expect(fa).toMatchObject({ deleted: true, content: "" });
		const last = (await mdb.decisionsOf(k.store.pool, a.factId)).at(-1);
		expect(last).toMatchObject({ decision: "delete", decider: "alice", commandId: delA.commandId });
		expect((await k.memory.delete(delA, alice, a.factId, 2)).existing).toBe(true);
		// b: revoked; d: revoked although the restore erased its confirmation too.
		expect((await k.memory.get(alice, b.factId)).state).toBe("revoked");
		const fd = await k.memory.get(alice, d.factId);
		expect(fd).toMatchObject({ state: "revoked", confirmer: "alice" });
		// A retried confirmation of d (erased by the restore) never revives it.
		expect(await code(k.memory.decide(confD, alice, d.factId, 1, "confirm", "", null))).not.toBe("OK");
		expect(await code(k.memory.decide(k.cmd(alice), alice, d.factId, fd.revision, "confirm", "", null))).toBe(
			"INVALID_TRANSITION",
		);
		// e: the restore erased the fact itself; a retried proposal never brings it back.
		expect(await code(k.memory.get(alice, e.factId))).toBe("NOT_FOUND");
		expect(await code(propose("alice prefers spaces in the restore drill", eProp))).toBe("INVALID_TRANSITION");
		expect(await mdb.getFact(k.store.pool, e.factId)).toBeUndefined();
		const restored = await k.inst.admin(
			"SELECT command_id FROM memory_removals WHERE state = 'restored' ORDER BY recorded_at",
		);
		expect(restored.rows.map((x) => x.command_id)).toEqual([
			delA.commandId,
			revB.commandId,
			revD.commandId,
			delE.commandId,
		]);

		// Recall and the projections follow: only g is recalled; the Store holds tombstones.
		const recalled = (await ask("restore drill")).facts.map((f) => f.factId);
		expect(recalled).toContain(g.factId);
		for (const id of [a.factId, b.factId, c.factId, d.factId, e.factId]) expect(recalled).not.toContain(id);
		await drainProjections(k);
		for (const id of [a.factId, b.factId]) {
			const stored = await k.memoryStore.get("tenant_a", id);
			expect(stored?.tombstone).toBe(true);
			expect(stored?.content).toBeUndefined();
			expect(await point(id)).toBeUndefined();
		}
		expect((await k.memory.get(alice, g.factId)).state).toBe("confirmed");
		// The next pass finds nothing more to restore.
		expect((await removals.pass(k.inventory)).restored).toBe(0);
	});

	it("closes memory again when the database's newest removal moves back under a running process", async () => {
		const h = await confirmed("alice prefers small diffs in the second drill");
		const t = await snapshot();
		const del = k.cmd(alice);
		await k.memory.delete(del, alice, h.factId, 2);
		await removals.pass(k.inventory);
		await restore(t);
		k.inventory.failListAfter = 0;
		await expect(removals.pass(k.inventory)).rejects.toBeInstanceOf(RemovalListIncomplete);
		expect(removals.ready()).toBe(false);
		expect(await code(k.memory.get(alice, h.factId))).toBe("UNAVAILABLE");
		k.inventory.resetFaults();
		expect((await removals.pass(k.inventory)).restored).toBe(1);
		expect(await code(k.memory.get(alice, h.factId))).toBe("NOT_FOUND");
	});

	it("never reads another database's records: the inventory is scoped by the database's removal scope", async () => {
		const foreign: RemovalRecord = {
			schemaVersion: 1,
			kind: "memory-removal",
			tenantId: "tenant_a",
			commandId: "foreign-delete",
			requestDigest: `sha256:${"f".repeat(64)}`,
			factId: (await confirmed("alice prefers the foreign drill")).factId,
			decision: "delete",
			decider: "alice",
			confirmer: "",
			reasonCode: "",
			fromRevision: 2,
			toRevision: 3,
			recordedAt: new Date().toISOString(),
		};
		const otherScope = "00000000-0000-4000-8000-000000000001";
		expect(await mdb.removalScope(k.store.pool)).not.toBe(otherScope);
		k.inventory.records.set(removalKeyOf(otherScope, foreign), removalBodyOf(foreign));
		expect((await removals.pass(k.inventory)).restored).toBe(0);
		expect((await k.memory.get(alice, foreign.factId)).state).toBe("confirmed");
	});

	it("a process with an inventory serves memory only after its first complete pass", async () => {
		const fresh = new MemoryRemovalInventory();
		const late = new Removals(
			k.store,
			() => fresh,
			() => k.removalBounds,
			k.clock,
			silentLogger,
			k.metrics,
		);
		late.setTarget(k.memory);
		expect(late.ready()).toBe(false);
		await late.tick();
		expect(late.ready()).toBe(true);
	});
});
