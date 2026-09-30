// P17-05: RecallFacts on real PostgreSQL 17 and Qdrant 1.19.0 (the
// Inference double stands in for BGE-M3): only confirmed, unexpired facts
// the caller may read now, with readable provenance, are candidates; both
// prefetch branches carry the allowed fact keys; a projection that still
// holds a revoked, deleted or expired fact never discloses it; the
// authority is rechecked before the reranker reads content and before
// return; an oversized allowed set is refused.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { HybridQuery, ScoredPoint } from "../src/adapters/qdrant.js";
import { Recall, type RecallRequest } from "../src/application/recall.js";
import { MemoryError } from "../src/domain/memory.js";
import type { RetrievalProfile } from "../src/domain/retrieval.js";
import type { Scope } from "../src/domain/source.js";
import { silentLogger } from "../src/log.js";
import { acceptGeneration, drainProjections, type Kit, startKit } from "./memorykit.js";
import { seedParsed } from "./rag.js";

let k: Kit;
let recall: Recall;
const spy: { last?: HybridQuery; after?: () => Promise<void> } = {};
const profile: RetrievalProfile = {
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
};

const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const bob: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "bob" };
const agent: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "agent-coder" };
const eve: Scope = { tenantId: "tenant_b", projectId: "proj_a", actorId: "alice" };
const agentB: Scope = { tenantId: "tenant_b", projectId: "proj_a", actorId: "agent-coder" };

async function fact(proposer: Scope, decider: Scope, over: Record<string, unknown> = {}) {
	const p = await k.memory.propose(k.cmd(proposer), proposer, {
		subjectType: "actor",
		subjectId: decider.actorId,
		content: "",
		sourceRefs: [],
		expiresAt: null,
		origin: "model",
		...over,
	} as never);
	return (await k.memory.decide(k.cmd(decider), decider, p.fact.factId, 1, "confirm", "", null)).fact;
}

const ask = (scope: Scope, query: string, extra: Partial<RecallRequest> = {}) =>
	recall.recall({
		scope,
		query,
		maxFacts: 5,
		retrievalProfileId: "hybrid-bge-m3-dev-v1",
		deadline: new Date(k.clock.now().getTime() + 30_000),
		...extra,
	});

async function withheld(stage: string): Promise<number> {
	return (await k.metrics.withheld.get()).values.find((v) => v.labels.stage === stage)?.value ?? 0;
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

let darkA = "";
let darkB = "";
let projectFact = "";
let sourced = "";

beforeAll(async () => {
	k = await startKit();
	await acceptGeneration(k);
	const spied = new Proxy(k.vectors, {
		get: (target, prop, recv) =>
			prop === "search"
				? async (collection: string, q: HybridQuery): Promise<ScoredPoint[]> => {
						spy.last = q;
						const out = await target.search(collection, q);
						await spy.after?.();
						return out;
					}
				: Reflect.get(target, prop, recv),
	});
	recall = new Recall(
		k.store,
		() => spied,
		() => k.inference,
		() => k.activeSpace.current,
		() => profile,
		() => k.bounds,
		k.clock,
		silentLogger,
		k.metrics,
	);
	await seedParsed(k.inst, k.objects, { sourceId: "src-style", tenantId: "tenant_a", texts: ["style guide"] });
	darkA = (await fact(agent, alice, { content: "alice prefers dark mode previews" })).factId;
	darkB = (await fact(agentB, eve, { content: "alice prefers dark mode previews" })).factId;
	projectFact = (
		await fact(agent, bob, {
			subjectType: "project",
			subjectId: "proj_a",
			content: "the project ships dark mode first",
		})
	).factId;
	sourced = (await fact(agent, alice, { content: "alice uses the brand dark palette", sourceRefs: ["src-style@1"] }))
		.factId;
	// A proposal that is never confirmed.
	await k.memory.propose(k.cmd(agent), agent, {
		subjectType: "actor",
		subjectId: "alice",
		content: "alice secretly hates dark mode",
		sourceRefs: [],
		expiresAt: null,
		origin: "model",
	});
	await drainProjections(k);
});

afterAll(async () => {
	await k?.stop();
});

describe("RecallFacts", () => {
	it("recalls the caller's confirmed facts with both prefetch branches filtered by the allowed fact keys", async () => {
		const r = await ask(alice, "dark mode previews");
		const ids = r.facts.map((f) => f.factId);
		expect(ids).toContain(darkA);
		expect(ids).toContain(projectFact);
		expect(ids).not.toContain(darkB);
		expect(r.facts.every((f) => f.state === "confirmed" && f.tenantId === "tenant_a")).toBe(true);
		expect(r.facts.map((f) => f.content)).not.toContain("alice secretly hates dark mode");
		const q = spy.last as HybridQuery;
		expect(q.prefetch.length).toBe(2);
		for (const f of [q.filter, ...q.prefetch.map((p) => p.filter)]) {
			expect(f.must).toContainEqual({ key: "tenant_id", match: { value: "tenant_a" } });
			expect(f.must).toContainEqual({ key: "kind", match: { value: "memory" } });
			const keys = f.must.find((c) => "key" in c && c.key === "memory_key");
			expect(keys).toBeDefined();
			expect(JSON.stringify(keys)).not.toContain(darkB);
		}
	});

	it("never contaminates tenants or actors: another tenant's identical fact and another actor's memory stay hidden", async () => {
		const e = await ask(eve, "dark mode previews");
		expect(e.facts.map((f) => f.factId)).toEqual([darkB]);
		const b = await ask(bob, "dark mode previews");
		expect(b.facts.map((f) => f.factId)).toEqual([projectFact]);
		const outsider = await ask({ ...alice, projectId: "proj_other" }, "dark mode previews");
		// A reader in another project sees none of proj_a's facts.
		expect(outsider).toMatchObject({ facts: [], noAnswer: true });
	});

	it("hides a fact whose provenance the caller can no longer read", async () => {
		expect((await ask(alice, "brand dark palette")).facts.map((f) => f.factId)).toContain(sourced);
		await k.inst.admin(
			"INSERT INTO source_acl_revisions (source_id, acl_revision, actor_id, command_id) VALUES ('src-style', 2, 'x', 'x')",
		);
		await k.inst.admin(
			"INSERT INTO source_acl (source_id, acl_revision, principal_type, principal_id) VALUES ('src-style', 2, 'actor', 'bob')",
		);
		await k.inst.admin("UPDATE sources SET acl_revision = 2 WHERE source_id = 'src-style'");
		expect((await ask(alice, "brand dark palette")).facts.map((f) => f.factId)).not.toContain(sourced);
		await k.inst.admin("UPDATE sources SET acl_revision = 1 WHERE source_id = 'src-style'");
	});

	it("never discloses a revoked, deleted or expired fact whose point is still in the index", async () => {
		const revoked = await fact(agent, alice, { content: "alice reviews on tuesdays" });
		const deleted = await fact(agent, alice, { content: "alice reviews on wednesdays" });
		const expiring = await fact(agent, alice, {
			content: "alice reviews on thursdays",
			expiresAt: new Date(k.clock.now().getTime() + 60_000),
		});
		await drainProjections(k);
		expect((await ask(alice, "reviews on weekdays")).facts.map((f) => f.factId)).toEqual(
			expect.arrayContaining([revoked.factId, deleted.factId, expiring.factId]),
		);
		// Decisions commit; the projections have not run yet, so the points remain.
		await k.memory.decide(k.cmd(alice), alice, revoked.factId, 2, "revoke", "", null);
		await k.memory.delete(k.cmd(alice), alice, deleted.factId, 2);
		k.clock.advance(120_000);
		const before = await k.inst.admin(
			"SELECT count(*)::int AS n FROM memory_projections WHERE target = 1 AND action = 'remove' AND state = 'pending'",
		);
		expect(before.rows[0].n).toBe(2);
		const r = await ask(alice, "reviews on weekdays");
		const ids = r.facts.map((f) => f.factId);
		expect(ids).not.toContain(revoked.factId);
		expect(ids).not.toContain(deleted.factId);
		expect(ids).not.toContain(expiring.factId);
		expect(r.noAnswer).toBe(true);
		await drainProjections(k);
	});

	it("rechecks before the reranker reads content and again before return", async () => {
		const f = await fact(agent, alice, { content: "alice names branches with ticket numbers" });
		await drainProjections(k);
		// Revoked after the index answered: the reranker never sees the content.
		spy.after = async () => {
			spy.after = undefined;
			await k.memory.decide(k.cmd(alice), alice, f.factId, 2, "revoke", "", null);
		};
		k.inference.rerankTexts = [];
		const read0 = await withheld("before_read");
		const r1 = await ask(alice, "branches ticket numbers");
		expect(await withheld("before_read")).toBe(read0 + 1);
		expect(r1.facts.map((x) => x.factId)).not.toContain(f.factId);
		expect(k.inference.rerankTexts.flat()).not.toContain("alice names branches with ticket numbers");

		const g = await fact(agent, alice, { content: "alice squashes commits before merging" });
		await drainProjections(k);
		k.inference.onRerank = async () => {
			k.inference.onRerank = undefined;
			await k.memory.delete(k.cmd(alice), alice, g.factId, 2);
		};
		const ret0 = await withheld("before_return");
		const r2 = await ask(alice, "squashes commits merging");
		expect(await withheld("before_return")).toBe(ret0 + 1);
		expect(r2.facts.map((x) => x.factId)).not.toContain(g.factId);
	});

	it("refuses an oversized allowed set, another profile and a passed deadline, and answers no evidence below the floor", async () => {
		k.bounds.maxAllowedFacts = 2;
		expect(await code(ask(alice, "dark"))).toBe("SCOPE_TOO_LARGE");
		k.bounds.maxAllowedFacts = 64;
		expect(await code(ask(alice, "dark", { retrievalProfileId: "other" }))).toBe("PROFILE_UNQUALIFIED");
		expect(await code(ask(alice, "dark", { deadline: new Date(k.clock.now().getTime() - 1) }))).toBe(
			"DEADLINE_EXCEEDED",
		);
		const none = await ask(alice, "quantum chromodynamics lattice");
		expect(none).toMatchObject({ facts: [], noAnswer: true });
		const stranger = await ask({ tenantId: "tenant_c", projectId: "", actorId: "zed" }, "dark mode");
		expect(stranger).toMatchObject({ facts: [], noAnswer: true });
		expect(stranger.compute.embeddings).toBe(0);
	});
});
