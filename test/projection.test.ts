// P17-04: the projections of MemoryFact to the PostgresStore and to every
// vector generation, on real PostgreSQL 17 (with the Store's vendor schema)
// and a real Qdrant 1.19.0: content only for a confirmed fact, content-free
// tombstones otherwise, the fact's current revision checked before and after
// every write so a stale write cannot revive a revoked fact, acceptance of
// the verified application only, backfill of a new generation and retry of
// failed rows under a new epoch.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { MemoryStorePort, StoredFact } from "../src/adapters/memorystore.js";
import type { QdrantIndex } from "../src/adapters/qdrant.js";
import { collectionOf } from "../src/domain/index.js";
import { memoryPointIdOf, projectionTaskIdOf } from "../src/domain/memory.js";
import type { Scope } from "../src/domain/source.js";
import { acceptGeneration, drainProjections, type Kit, ledger, runProjection, startKit } from "./memorykit.js";

let k: Kit;
const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const agent: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "agent-coder" };
let n = 0;

async function confirmed(content = `alice prefers compact tables ${++n}`) {
	const p = await k.memory.propose(k.cmd(agent), agent, {
		subjectType: "actor",
		subjectId: "alice",
		content,
		sourceRefs: [],
		expiresAt: null,
		origin: "model",
	});
	return (await k.memory.decide(k.cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)).fact;
}

async function point(generation: number, factId: string) {
	return (await k.vectors.retrieve(collectionOf(generation), [memoryPointIdOf(factId)])).get(memoryPointIdOf(factId));
}

beforeAll(async () => {
	k = await startKit();
	await acceptGeneration(k);
});

afterAll(async () => {
	await k?.stop();
});

describe("memory projections", () => {
	it("applies a confirmed fact to the Store and the accepted generation, and removes it as a tombstone on revocation", async () => {
		const f = await confirmed();
		const rows = await ledger(k, f.factId);
		expect(rows.map((r) => [r.target, r.action, r.state])).toEqual([
			[0, "apply", "pending"],
			[1, "apply", "pending"],
		]);
		expect(await drainProjections(k)).toBe(2);
		const stored = await k.memoryStore.get("tenant_a", f.factId);
		expect(stored).toMatchObject({ revision: 2, tombstone: false, content: f.content, contentDigest: f.contentDigest });
		const pt = await point(1, f.factId);
		expect(pt?.payload).toMatchObject({
			kind: "memory",
			fact_id: f.factId,
			fact_revision: 2,
			memory_key: `${f.factId}@2`,
		});
		expect(pt?.payload).not.toHaveProperty("content");
		expect(
			(await ledger(k, f.factId)).every((r) => r.state === "accepted" && r.manifestDigest.startsWith("sha256:")),
		).toBe(true);

		await k.memory.decide(k.cmd(alice), alice, f.factId, 2, "revoke", "", null);
		expect((await ledger(k, f.factId)).map((r) => [r.target, r.action, r.factRevision])).toEqual([
			[0, "remove", 3],
			[1, "remove", 3],
		]);
		await drainProjections(k);
		const tomb = await k.memoryStore.get("tenant_a", f.factId);
		expect(tomb).toEqual({ factId: f.factId, revision: 3, state: "revoked", tombstone: true, contentDigest: "" });
		expect(await point(1, f.factId)).toBeUndefined();
	});

	it("never lets a stale Store write revive a revoked fact: the late writer re-applies the current state", async () => {
		const f = await confirmed();
		// The Store task of revision 2 is claimed and pauses inside its put.
		let release!: () => void;
		let paused!: () => void;
		const inPut = new Promise<void>((r) => {
			paused = r;
		});
		const gate = new Promise<void>((r) => {
			release = r;
		});
		let hold = true;
		const slow: MemoryStorePort = {
			get: (t, id) => k.memoryStore.get(t, id),
			list: (t, o, l) => k.memoryStore.list(t, o, l),
			close: async () => undefined,
			put: async (t, v: StoredFact) => {
				if (hold && !v.tombstone) {
					hold = false;
					paused();
					await gate;
				}
				return k.memoryStore.put(t, v);
			},
		};
		k.setMemoryStore(slow);
		const stale = runProjection(k, projectionTaskIdOf(f.factId, 0));
		await inPut;
		// Revocation commits and its own Store task writes the tombstone first.
		await k.memory.decide(k.cmd(alice), alice, f.factId, 2, "revoke", "", null);
		const fresh = await runProjection(k, projectionTaskIdOf(f.factId, 0));
		expect(fresh.accepted).toBe(true);
		expect((await k.memoryStore.get("tenant_a", f.factId))?.tombstone).toBe(true);
		release();
		const late = await stale;
		// The late writer wrote revision 2's content, saw revision 3 and wrote the tombstone again.
		expect(late.answer).toEqual({ state: "failed", failureCode: "SUPERSEDED" });
		expect(late.accepted).toBe(false);
		const final = await k.memoryStore.get("tenant_a", f.factId);
		expect(final).toMatchObject({ revision: 3, tombstone: true });
		expect(final?.content).toBeUndefined();
		k.setMemoryStore(k.memoryStore);
		await drainProjections(k);
	});

	it("never lets a stale vector write revive a revoked fact", async () => {
		const f = await confirmed();
		await runProjection(k, projectionTaskIdOf(f.factId, 0));
		let release!: () => void;
		let paused!: () => void;
		const inUpsert = new Promise<void>((r) => {
			paused = r;
		});
		const gate = new Promise<void>((r) => {
			release = r;
		});
		const slow = new Proxy(k.vectors, {
			get: (target, prop, recv) =>
				prop === "upsert"
					? async (...args: Parameters<QdrantIndex["upsert"]>) => {
							paused();
							await gate;
							return target.upsert(...args);
						}
					: Reflect.get(target, prop, recv),
		});
		k.setVectors(slow);
		const stale = runProjection(k, projectionTaskIdOf(f.factId, 1));
		await inUpsert;
		k.setVectors(k.vectors);
		await k.memory.decide(k.cmd(alice), alice, f.factId, 2, "revoke", "", null);
		await drainProjections(k);
		expect(await point(1, f.factId)).toBeUndefined();
		k.setVectors(slow);
		release();
		const late = await stale;
		k.setVectors(k.vectors);
		expect(late.answer).toEqual({ state: "failed", failureCode: "SUPERSEDED" });
		expect(await point(1, f.factId)).toBeUndefined();
	});

	it("refuses a result that is not the verified application and never accepts a superseded one", async () => {
		const f = await confirmed();
		const row = (await ledger(k, f.factId))[0];
		const r = await runProjection(k, row?.taskId as string, async () => {
			// A newer decision lands between verification and submission.
			await k.memory.decide(k.cmd(alice), alice, f.factId, 2, "revoke", "", null);
		});
		expect(r.accepted).toBe(false);
		await drainProjections(k);
		expect((await ledger(k, f.factId)).every((x) => x.factRevision === 3 && x.state === "accepted")).toBe(true);
	});

	it("retries a failed target under a new epoch after the delay and backfills a new generation", async () => {
		k.setMemoryStore(undefined);
		const f = await confirmed();
		await drainProjections(k);
		const failed = (await ledger(k, f.factId)).find((r) => r.target === 0);
		expect(failed).toMatchObject({ state: "failed", failureCode: "STORE_UNAVAILABLE", epoch: 0 });
		k.setMemoryStore(k.memoryStore);
		k.bounds.retryDelayMs = 3_600_000;
		await k.projector.reconcile();
		expect((await ledger(k, f.factId)).find((r) => r.target === 0)?.state).toBe("failed");
		k.bounds.retryDelayMs = 0;
		await k.projector.reconcile();
		expect((await ledger(k, f.factId)).find((r) => r.target === 0)).toMatchObject({ state: "pending", epoch: 1 });
		await drainProjections(k);
		expect((await k.memoryStore.get("tenant_a", f.factId))?.revision).toBe(2);

		// A rebuild of the space: generation 2 stays building until every live fact is projected there.
		const live = await k.inst.admin(
			"SELECT count(*)::int AS n FROM memory_facts WHERE state = 'confirmed' AND NOT deleted",
		);
		expect(await k.indexer.createGeneration()).toBe(2);
		await k.indexer.reconcile();
		await k.projector.reconcile();
		await k.indexer.reconcile();
		const gen2 = await k.inst.admin("SELECT state FROM index_generations WHERE generation = 2");
		expect(gen2.rows[0].state).toBe("building");
		expect(await drainProjections(k)).toBe(live.rows[0].n);
		for (let i = 0; i < 3; i++) await k.indexer.reconcile();
		const states = await k.inst.admin("SELECT generation::int AS g, state FROM index_generations ORDER BY generation");
		expect(states.rows).toEqual([
			{ g: 1, state: "retired" },
			{ g: 2, state: "accepted" },
		]);
		expect((await point(2, f.factId))?.payload?.fact_revision).toBe(2);
		// A revocation now removes the fact from the retired generation too.
		await k.memory.decide(k.cmd(alice), alice, f.factId, 2, "revoke", "", null);
		expect((await ledger(k, f.factId)).map((r) => r.target)).toEqual([0, 1, 2]);
		await drainProjections(k);
		expect(await point(1, f.factId)).toBeUndefined();
		expect(await point(2, f.factId)).toBeUndefined();
	});
});
