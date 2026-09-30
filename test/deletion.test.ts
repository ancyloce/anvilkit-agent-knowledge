// P17-06: expiry, deletion and recovery on real PostgreSQL 17 (with the
// Store's vendor schema) and Qdrant 1.19.0. Deletion ends readability in its
// own transaction and erases the content, then the Store and every vector
// generation (retired ones included) apply its tombstone; purged_at is set
// only when every target accepted it. The reviewed expiry rule is a policy
// decision with its revision. A rebuild restores projections from the
// authoritative facts and never revives a revoked, expired or deleted fact.
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { collectionOf } from "../src/domain/index.js";
import { MemoryError, memoryPointIdOf } from "../src/domain/memory.js";
import type { Scope } from "../src/domain/source.js";
import { acceptGeneration, drainProjections, type Kit, ledger, runProjection, startKit } from "./memorykit.js";

let k: Kit;
const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const agent: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "agent-coder" };
let n = 0;

async function propose(content = `alice keeps a changelog ${++n}`, expiresAt: Date | null = null) {
	return (
		await k.memory.propose(k.cmd(agent), agent, {
			subjectType: "actor",
			subjectId: "alice",
			content,
			sourceRefs: [],
			expiresAt,
			origin: "model",
		})
	).fact;
}

async function confirmed(content?: string, expiresAt: Date | null = null) {
	const p = await propose(content, expiresAt);
	return (await k.memory.decide(k.cmd(alice), alice, p.factId, 1, "confirm", "", null)).fact;
}

async function point(generation: number, factId: string) {
	return (await k.vectors.retrieve(collectionOf(generation), [memoryPointIdOf(factId)])).get(memoryPointIdOf(factId));
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

async function events(factId: string): Promise<Record<string, string>[]> {
	const r = await k.inst.admin('SELECT payload::text AS p FROM outbox ORDER BY "offset"');
	return r.rows
		.map((x) => JSON.parse(Buffer.from(JSON.parse(x.p).payload, "base64").toString()))
		.filter((e) => e.aggregateId === factId)
		.map((e) => e.payload);
}

beforeAll(async () => {
	k = await startKit();
	await acceptGeneration(k);
});

afterAll(async () => {
	await k?.stop();
});

describe("deletion across every projection", () => {
	it("ends readability at once, erases the content and purges the Store and every generation, retired ones included", async () => {
		const f = await confirmed("alice lives in berlin");
		await drainProjections(k);
		// A rebuild of the space retires generation 1 after the fact is projected into generation 2.
		await k.indexer.createGeneration();
		await k.indexer.reconcile();
		await k.projector.reconcile();
		await drainProjections(k);
		for (let i = 0; i < 3; i++) await k.indexer.reconcile();
		expect((await point(1, f.factId))?.payload?.fact_revision).toBe(2);
		expect((await point(2, f.factId))?.payload?.fact_revision).toBe(2);
		const states = await k.inst.admin("SELECT generation::int AS g, state FROM index_generations ORDER BY generation");
		expect(states.rows.map((x) => x.state)).toEqual(["retired", "accepted"]);

		const d = await k.memory.delete(k.cmd(alice), alice, f.factId, 2);
		expect(d.fact).toMatchObject({ deleted: true, revision: 3, content: "", contentDigest: f.contentDigest });
		expect(await code(k.memory.get(alice, f.factId))).toBe("NOT_FOUND");
		expect(
			(await k.memory.list(alice, { subjectType: "", subjectId: "", state: "" }, "", 200)).facts.map((x) => x.factId),
		).not.toContain(f.factId);
		const row = await k.inst.admin("SELECT content, deleted, purged_at FROM memory_facts WHERE fact_id = $1", [
			f.factId,
		]);
		expect(row.rows[0]).toMatchObject({ content: "", deleted: true, purged_at: null });
		expect((await events(f.factId)).map((e) => e.state)).toEqual(["confirmed", "deleted"]);
		const history = await k.inst.admin(
			"SELECT decision, authority FROM memory_decisions WHERE fact_id = $1 ORDER BY to_revision",
			[f.factId],
		);
		expect(history.rows.map((x) => x.decision)).toEqual(["propose", "confirm", "delete"]);
		expect((await ledger(k, f.factId)).map((r) => [r.target, r.action])).toEqual([
			[0, "remove"],
			[1, "remove"],
			[2, "remove"],
		]);
		// Two of three targets done: not purged yet.
		const rows = await ledger(k, f.factId);
		await runProjection(k, rows[0]?.taskId as string);
		await runProjection(k, rows[1]?.taskId as string);
		expect(
			(await k.inst.admin("SELECT purged_at FROM memory_facts WHERE fact_id = $1", [f.factId])).rows[0].purged_at,
		).toBeNull();
		await runProjection(k, rows[2]?.taskId as string);
		expect(
			(await k.inst.admin("SELECT purged_at FROM memory_facts WHERE fact_id = $1", [f.factId])).rows[0].purged_at,
		).not.toBeNull();
		expect(await k.memoryStore.get("tenant_a", f.factId)).toEqual({
			factId: f.factId,
			revision: 3,
			state: "deleted",
			tombstone: true,
			contentDigest: "",
		});
		expect(await point(1, f.factId)).toBeUndefined();
		expect(await point(2, f.factId)).toBeUndefined();
		// A deleted fact takes no further decision.
		expect(await code(k.memory.decide(k.cmd(alice), alice, f.factId, 3, "revoke", "", null))).toBe("NOT_FOUND");
		expect(await code(k.memory.delete(k.cmd(alice), alice, f.factId, 3))).toBe("NOT_FOUND");
	});

	it("purges a never-projected proposal in its deletion transaction", async () => {
		const p = await propose();
		await k.memory.delete(k.cmd(alice), alice, p.factId, 1);
		const row = await k.inst.admin("SELECT purged_at, content FROM memory_facts WHERE fact_id = $1", [p.factId]);
		expect(row.rows[0].purged_at).not.toBeNull();
		expect(row.rows[0].content).toBe("");
		expect(await ledger(k, p.factId)).toEqual([]);
		expect(await events(p.factId)).toEqual([]);
	});
});

describe("the reviewed expiry rule", () => {
	it("expires due facts as a policy decision with its revision, publishes it and removes the projections", async () => {
		const f = await confirmed(undefined, new Date(k.clock.now().getTime() + 60_000));
		const keep = await confirmed();
		await drainProjections(k);
		expect(await k.memory.expireDue(100)).toBe(0);
		k.clock.advance(61_000);
		expect(await k.memory.expireDue(100)).toBe(1);
		expect(await k.memory.expireDue(100)).toBe(0);
		expect(await k.memory.get(alice, f.factId)).toMatchObject({ state: "expired", revision: 3 });
		expect((await k.memory.get(alice, keep.factId)).state).toBe("confirmed");
		const d = await k.inst.admin(
			"SELECT decision, authority, decider, policy_revision FROM memory_decisions WHERE fact_id = $1 AND to_revision = 3",
			[f.factId],
		);
		expect(d.rows[0]).toEqual({
			decision: "expire",
			authority: "policy",
			decider: "policy:memory-expiry-v1",
			policy_revision: "1",
		});
		expect((await events(f.factId)).map((e) => e.state)).toEqual(["confirmed", "expired"]);
		await drainProjections(k);
		expect(await k.memoryStore.get("tenant_a", f.factId)).toMatchObject({
			revision: 3,
			state: "expired",
			tombstone: true,
		});
		expect(await point(2, f.factId)).toBeUndefined();
		expect(await code(k.memory.decide(k.cmd(alice), alice, f.factId, 3, "confirm", "", null))).toBe(
			"INVALID_TRANSITION",
		);
	});
});

describe("recovery from confirmed current facts", () => {
	it("rebuilds a lost Store and a lost point without reviving any removal", async () => {
		const live = await confirmed("alice pairs on fridays");
		const revoked = await confirmed("alice pairs on mondays");
		await k.memory.decide(k.cmd(alice), alice, revoked.factId, 2, "revoke", "", null);
		await drainProjections(k);
		// The Store's data is lost (a vendor restore to empty).
		const c = new pg.Client({ connectionString: k.inst.storeUrl });
		await c.connect();
		await c.query('DELETE FROM "memory_store".store');
		await c.end();
		expect(await k.memoryStore.get("tenant_a", live.factId)).toBeUndefined();
		const requested = await k.projector.rebuild(0);
		expect(requested).toBeGreaterThanOrEqual(2);
		await drainProjections(k);
		expect(await k.memoryStore.get("tenant_a", live.factId)).toMatchObject({
			revision: 2,
			tombstone: false,
			content: "alice pairs on fridays",
		});
		const tomb = await k.memoryStore.get("tenant_a", revoked.factId);
		expect(tomb).toMatchObject({ revision: 3, state: "revoked", tombstone: true });
		expect(tomb?.content).toBeUndefined();
		// Every fact that was deleted earlier is rebuilt content-free.
		const deleted = await k.inst.admin("SELECT fact_id FROM memory_facts WHERE deleted AND purged_at IS NOT NULL");
		for (const r of deleted.rows) {
			const v = await k.memoryStore.get("tenant_a", r.fact_id as string);
			if (v) expect(v).toMatchObject({ tombstone: true, contentDigest: "" });
		}

		// A point is lost from the accepted generation; the rebuild restores it, never the revoked one.
		await k.vectors.deleteIds(collectionOf(2), [memoryPointIdOf(live.factId)]);
		await k.projector.rebuild(2);
		await drainProjections(k);
		expect((await point(2, live.factId))?.payload?.fact_revision).toBe(2);
		expect(await point(2, revoked.factId)).toBeUndefined();
		const epochs = await ledger(k, live.factId);
		expect(epochs.every((r) => r.state === "accepted")).toBe(true);
		expect(epochs.find((r) => r.target === 2)?.epoch).toBe(1);
	});
});
