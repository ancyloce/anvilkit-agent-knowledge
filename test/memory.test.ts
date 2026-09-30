// P17-03: MemoryFact proposals and decisions on a real PostgreSQL 17 —
// models and workers only propose, decisions are a user's under the
// expected revision, provenance is checked against the current ACL, and the
// fact, its decision, its event and its projection requests commit or roll
// back together.
import { Registry } from "prom-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { Memory, type Proposal } from "../src/application/memory.js";
import type { Tasks } from "../src/application/tasks.js";
import { MemoryError } from "../src/domain/memory.js";
import type { Command, Scope } from "../src/domain/source.js";
import { silentLogger } from "../src/log.js";
import { Metrics } from "../src/metrics.js";
import { bounds, FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";
import { seedParsed } from "./rag.js";

const clock = new FakeClock(new Date("2026-09-29T12:00:00Z"));
let inst: Instance;
let tasks: Tasks;
let store: Store;
let memory: Memory;
let seq = 0;

const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const bob: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "bob" };
const agent: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "agent-coder" };
const mallory: Scope = { tenantId: "tenant_b", projectId: "proj_a", actorId: "alice" };

const cmd = (scope: Scope, digest = `sha256:${"c".repeat(64)}`): Command => ({
	tenantId: scope.tenantId,
	commandId: `mem-cmd-${++seq}`,
	actorId: scope.actorId,
	requestDigest: digest,
});

const proposal = (over: Partial<Proposal> = {}): Proposal => ({
	subjectType: "actor",
	subjectId: "alice",
	content: `alice prefers dark previews ${++seq}`,
	sourceRefs: [],
	expiresAt: null,
	origin: "model",
	...over,
});

async function code(p: Promise<unknown>): Promise<string> {
	try {
		await p;
		return "OK";
	} catch (err) {
		if (err instanceof MemoryError) return err.code;
		throw err;
	}
}

beforeAll(async () => {
	inst = await startInstance();
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	memory = new Memory(
		store,
		tasks,
		() => undefined,
		() => ({ maxAllowedFacts: 64, maxProjectionEpochs: 3, retryDelayMs: 1000 }),
		clock,
		silentLogger,
		new Metrics(new Registry()),
	);
	const objects = new MemoryObjects();
	await seedParsed(inst, objects, { sourceId: "src-brief", tenantId: "tenant_a", texts: ["brief"] });
	await seedParsed(inst, objects, {
		sourceId: "src-private",
		tenantId: "tenant_a",
		access: [{ principalType: "actor", principalId: "bob" }],
		texts: ["private"],
	});
});

afterAll(async () => {
	await store?.pool.end();
	await inst?.stop();
});

describe("MemoryFact proposals", () => {
	it("records a model proposal with its origin, exact provenance and a propose decision, idempotently", async () => {
		const c = cmd(agent);
		const p = proposal({ sourceRefs: ["src-brief@1", "src-brief@1"] });
		const a = await memory.propose(c, agent, p);
		expect(a.existing).toBe(false);
		expect(a.fact).toMatchObject({ state: "proposed", revision: 1, origin: "model", proposer: "agent-coder" });
		expect(a.fact.sourceRefs).toEqual(["src-brief@1"]);
		expect(a.fact.scopeId).toBe("proj_a");
		const again = await memory.propose(c, agent, p);
		expect(again).toMatchObject({ existing: true, fact: { factId: a.fact.factId } });
		expect(await code(memory.propose({ ...c, requestDigest: `sha256:${"d".repeat(64)}` }, agent, p))).toBe(
			"COMMAND_CONFLICT",
		);
		const rows = await inst.admin("SELECT decision, authority, decider FROM memory_decisions WHERE fact_id = $1", [
			a.fact.factId,
		]);
		expect(rows.rows).toEqual([{ decision: "propose", authority: "model", decider: "agent-coder" }]);
		// Proposals are not projected and publish nothing.
		const reqs = await inst.admin("SELECT count(*)::int AS n FROM background_requests WHERE authorization_ref = $1", [
			`memory:${a.fact.factId}`,
		]);
		expect(reqs.rows[0].n).toBe(0);
	});

	it("refuses provenance the proposer cannot read now, a stale revision, a foreign subject and a past expiry", async () => {
		expect(await code(memory.propose(cmd(agent), agent, proposal({ sourceRefs: ["src-private@1"] })))).toBe(
			"PROVENANCE_STALE",
		);
		expect(await code(memory.propose(cmd(agent), agent, proposal({ sourceRefs: ["src-brief@2"] })))).toBe(
			"PROVENANCE_STALE",
		);
		expect(await code(memory.propose(cmd(agent), agent, proposal({ sourceRefs: ["src-missing@1"] })))).toBe(
			"PROVENANCE_STALE",
		);
		expect(await code(memory.propose(cmd(agent), agent, proposal({ sourceRefs: ["src-brief"] })))).toBe(
			"INVALID_ARGUMENT",
		);
		expect(
			await code(memory.propose(cmd(agent), agent, proposal({ subjectType: "project", subjectId: "proj_other" }))),
		).toBe("FORBIDDEN");
		expect(
			await code(memory.propose(cmd(agent), agent, proposal({ subjectType: "tenant", subjectId: "tenant_b" }))),
		).toBe("FORBIDDEN");
		expect(await code(memory.propose(cmd(agent), agent, proposal({ subjectType: "secret" })))).toBe("INVALID_ARGUMENT");
		expect(
			await code(memory.propose(cmd(agent), agent, proposal({ expiresAt: new Date("2026-09-29T11:00:00Z") }))),
		).toBe("EXPIRED");
		// The command must be the scope's own identity.
		expect(await code(memory.propose({ ...cmd(agent), actorId: "alice" }, agent, proposal()))).toBe("FORBIDDEN");
	});
});

describe("MemoryFact decisions", () => {
	it("never lets a model or worker identity decide: not its own proposal, not another one", async () => {
		const byModel = await memory.propose(cmd(agent), agent, proposal({ subjectType: "project", subjectId: "proj_a" }));
		expect(await code(memory.decide(cmd(agent), agent, byModel.fact.factId, 1, "confirm", "", null))).toBe("FORBIDDEN");
		const byUser = await memory.propose(
			cmd(bob),
			bob,
			proposal({ subjectType: "project", subjectId: "proj_a", origin: "user" }),
		);
		expect(await code(memory.decide(cmd(agent), agent, byUser.fact.factId, 1, "confirm", "", null))).toBe("FORBIDDEN");
		expect(await code(memory.delete(cmd(agent), agent, byUser.fact.factId, 1))).toBe("FORBIDDEN");
		const worker: Scope = { ...agent, actorId: "worker-7" };
		const byWorker = await memory.propose(
			cmd(worker),
			worker,
			proposal({ subjectType: "project", subjectId: "proj_a", origin: "worker" }),
		);
		expect(await code(memory.decide(cmd(worker), worker, byWorker.fact.factId, 1, "confirm", "", null))).toBe(
			"FORBIDDEN",
		);
		// The facts stay proposed; no decision of a model or worker authority exists beyond proposals.
		expect((await memory.get(bob, byModel.fact.factId)).state).toBe("proposed");
		const bad = await inst.admin(
			"SELECT count(*)::int AS n FROM memory_decisions WHERE authority <> 'user' AND decision <> 'propose' AND authority <> 'policy'",
		);
		expect(bad.rows[0].n).toBe(0);
		// A user proposes and confirms their own memory.
		const own = await memory.propose(cmd(alice), alice, proposal({ origin: "user" }));
		expect((await memory.decide(cmd(alice), alice, own.fact.factId, 1, "confirm", "", null)).fact.state).toBe(
			"confirmed",
		);
	});

	it("confirms under the expected revision: fact, decision, event and projection requests commit together", async () => {
		const p = await memory.propose(cmd(agent), agent, proposal({ sourceRefs: ["src-brief@1"] }));
		const expires = new Date("2026-12-01T00:00:00Z");
		const d = await memory.decide(cmd(alice), alice, p.fact.factId, 1, "confirm", "reviewed", expires);
		expect(d.fact).toMatchObject({ state: "confirmed", revision: 2, confirmer: "alice", expiresAt: expires });
		const dec = await inst.admin(
			"SELECT decision, authority, from_revision::int AS f, to_revision::int AS t, source_refs, reason_code FROM memory_decisions WHERE fact_id = $1 ORDER BY to_revision",
			[p.fact.factId],
		);
		expect(dec.rows[1]).toEqual({
			decision: "confirm",
			authority: "user",
			f: 1,
			t: 2,
			source_refs: ["src-brief@1"],
			reason_code: "reviewed",
		});
		const events = await inst.admin(
			"SELECT metadata::text, payload::text FROM outbox WHERE payload::text LIKE '%memory.fact-confirmed%'",
		);
		expect(events.rows.length).toBeGreaterThan(0);
		const env = JSON.parse(
			Buffer.from(JSON.parse(events.rows[events.rows.length - 1].payload).payload, "base64").toString(),
		);
		expect(env.payload).toEqual({
			kind: "memory.fact-confirmed",
			factId: p.fact.factId,
			revision: "2",
			state: "confirmed",
		});
		expect(JSON.stringify(env)).not.toContain("dark previews");
		const proj = await inst.admin(
			"SELECT target::int AS target, fact_revision::int AS rev, action, state FROM memory_projections WHERE fact_id = $1",
			[p.fact.factId],
		);
		expect(proj.rows).toEqual([{ target: 0, rev: 2, action: "apply", state: "pending" }]);
		const task = await inst.admin(
			"SELECT task_kind, result_profile, authorization_ref, state FROM background_requests WHERE task_id = $1",
			[`memproj-${p.fact.factId}-t0`],
		);
		expect(task.rows).toEqual([
			{
				task_kind: "memory-project",
				result_profile: "memory-project-v1",
				authorization_ref: `memory:${p.fact.factId}`,
				state: "pending",
			},
		]);
		// Replay answers the same decision; the same command for another decision conflicts.
		const c = cmd(alice);
		const q = await memory.propose(cmd(agent), agent, proposal());
		await memory.decide(c, alice, q.fact.factId, 1, "reject", "", null);
		expect((await memory.decide(c, alice, q.fact.factId, 1, "reject", "", null)).existing).toBe(true);
		expect(await code(memory.decide(c, alice, q.fact.factId, 1, "confirm", "", null))).toBe("COMMAND_CONFLICT");
	});

	it("serializes racing decisions: one wins the revision, stale ones are refused, revoked facts never return", async () => {
		for (let i = 0; i < 5; i++) {
			const p = await memory.propose(cmd(agent), agent, proposal());
			const results = await Promise.all([
				code(memory.decide(cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)),
				code(memory.decide(cmd(alice), alice, p.fact.factId, 1, "reject", "", null)),
				code(memory.decide(cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)),
			]);
			expect(results.filter((r) => r === "OK")).toHaveLength(1);
			expect(results.filter((r) => r !== "OK").every((r) => r === "REVISION_MISMATCH")).toBe(true);
			const f = await memory.get(alice, p.fact.factId);
			expect(f.revision).toBe(2);
			if (f.state === "confirmed") {
				// Revoke and a late confirmation carrying the old revision race.
				const race = await Promise.all([
					code(memory.decide(cmd(alice), alice, p.fact.factId, 2, "revoke", "", null)),
					code(memory.decide(cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)),
				]);
				expect(race).toEqual(["OK", "REVISION_MISMATCH"]);
				expect(await code(memory.decide(cmd(alice), alice, p.fact.factId, 3, "confirm", "", null))).toBe(
					"INVALID_TRANSITION",
				);
				expect((await memory.get(alice, p.fact.factId)).state).toBe("revoked");
			}
		}
	});

	it("applies the conflict, expiry and provenance rules at confirmation", async () => {
		const content = `alice builds with tabs ${++seq}`;
		const a = await memory.propose(cmd(agent), agent, proposal({ content }));
		const b = await memory.propose(cmd(agent), agent, proposal({ content }));
		await memory.decide(cmd(alice), alice, a.fact.factId, 1, "confirm", "", null);
		expect(await code(memory.decide(cmd(alice), alice, b.fact.factId, 1, "confirm", "", null))).toBe("FACT_CONFLICT");
		const c = await memory.propose(cmd(agent), agent, proposal());
		expect(
			await code(memory.decide(cmd(alice), alice, c.fact.factId, 1, "confirm", "", new Date("2026-01-01T00:00:00Z"))),
		).toBe("EXPIRED");
		expect(
			await code(memory.decide(cmd(alice), alice, c.fact.factId, 1, "reject", "", new Date("2027-01-01T00:00:00Z"))),
		).toBe("INVALID_ARGUMENT");
		// Provenance the decider can no longer read at its current revision.
		const d = await memory.propose(cmd(agent), agent, proposal({ sourceRefs: ["src-brief@1"] }));
		await inst.admin(
			"INSERT INTO source_acl_revisions (source_id, acl_revision, actor_id, command_id) VALUES ('src-brief', 2, 'x', 'x')",
		);
		await inst.admin(
			"INSERT INTO source_acl (source_id, acl_revision, principal_type, principal_id) VALUES ('src-brief', 2, 'actor', 'bob')",
		);
		await inst.admin("UPDATE sources SET acl_revision = 2 WHERE source_id = 'src-brief'");
		expect(await code(memory.decide(cmd(alice), alice, d.fact.factId, 1, "confirm", "", null))).toBe(
			"PROVENANCE_STALE",
		);
		expect((await memory.get(alice, d.fact.factId)).state).toBe("proposed");
		await inst.admin("UPDATE sources SET acl_revision = 1 WHERE source_id = 'src-brief'");
	});

	it("shows and lets decide only the facts the reader may read", async () => {
		const aboutAlice = await memory.propose(cmd(agent), agent, proposal());
		expect(await code(memory.get(bob, aboutAlice.fact.factId))).toBe("NOT_FOUND");
		expect(await code(memory.get(mallory, aboutAlice.fact.factId))).toBe("NOT_FOUND");
		expect(await code(memory.decide(cmd(bob), bob, aboutAlice.fact.factId, 1, "confirm", "", null))).toBe("NOT_FOUND");
		expect(await code(memory.decide(cmd(mallory), mallory, aboutAlice.fact.factId, 1, "confirm", "", null))).toBe(
			"NOT_FOUND",
		);
		const otherProject = await memory.get({ ...alice, projectId: "proj_b" }, aboutAlice.fact.factId).catch((e) => e);
		expect(otherProject).toBeInstanceOf(MemoryError);
		const tenantFact = await memory.propose(
			cmd(agent),
			agent,
			proposal({ subjectType: "tenant", subjectId: "tenant_a" }),
		);
		expect((await memory.get({ ...bob, projectId: "" }, tenantFact.fact.factId)).scopeId).toBe("");
		const listed = await memory.list(bob, { subjectType: "", subjectId: "", state: "" }, "", 200);
		expect(
			listed.facts.every((f) => f.tenantId === "tenant_a" && (f.subjectType !== "actor" || f.subjectId === "bob")),
		).toBe(true);
		expect(listed.facts.map((f) => f.factId)).toContain(tenantFact.fact.factId);
		const page = await memory.list(alice, { subjectType: "actor", subjectId: "alice", state: "proposed" }, "", 2);
		expect(page.facts).toHaveLength(2);
		const next = await memory.list(
			alice,
			{ subjectType: "actor", subjectId: "alice", state: "proposed" },
			page.nextCursor,
			2,
		);
		expect(next.facts.length).toBeGreaterThan(0);
		expect(next.facts.every((f) => f.factId > (page.facts[1]?.factId ?? ""))).toBe(true);
	});

	it("rolls the fact, decision and event back when the projection request cannot commit", async () => {
		const p = await memory.propose(cmd(agent), agent, proposal());
		const before = await inst.admin("SELECT count(*)::int AS n FROM outbox");
		tasks.setBounds({ ...bounds, maxInputBytes: 8 });
		try {
			const outcome = await code(memory.decide(cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)).catch(
				(err: Error) => err.message,
			);
			expect(outcome).toMatch(/^INPUT_TOO_LARGE/);
		} finally {
			tasks.setBounds(bounds);
		}
		expect((await memory.get(alice, p.fact.factId)).state).toBe("proposed");
		const dec = await inst.admin("SELECT count(*)::int AS n FROM memory_decisions WHERE fact_id = $1", [p.fact.factId]);
		expect(dec.rows[0].n).toBe(1);
		const after = await inst.admin("SELECT count(*)::int AS n FROM outbox");
		expect(after.rows[0].n).toBe(before.rows[0].n);
		expect((await memory.decide(cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)).fact.revision).toBe(2);
	});
});
