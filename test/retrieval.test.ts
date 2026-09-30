// P16-03/P16-04: snapshots and hybrid retrieval under the current ACL, on
// real PostgreSQL 17 and a real Qdrant 1.19.0 (the Inference double of
// rag.ts stands in for BGE-M3; quality is measured by tests/evals/rag).

import { QdrantClient } from "@qdrant/js-client-rest";
import { Registry } from "prom-client";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import type { HybridQuery, QdrantIndex, ScoredPoint } from "../src/adapters/qdrant.js";
import { Indexer } from "../src/application/indexer.js";
import { Retrieval, type SearchRequest } from "../src/application/retrieval.js";
import { Snapshots } from "../src/application/snapshots.js";
import type { Tasks } from "../src/application/tasks.js";
import { collectionOf } from "../src/domain/index.js";
import { RetrievalError, type RetrievalProfile } from "../src/domain/retrieval.js";
import type { Command, Scope } from "../src/domain/source.js";
import { silentLogger } from "../src/log.js";
import { Metrics } from "../src/metrics.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";
import { apiKey, HashInference, indexSeeded, qdrantIndex, seedParsed, space, startQdrant } from "./rag.js";

const clock = new FakeClock(new Date("2026-09-28T12:00:00Z"));
const objects = new MemoryObjects();
const inference = new HashInference();
const metrics = new Metrics(new Registry());
let inst: Instance;
let qdrant: { container: StartedTestContainer; url: string };
let tasks: Tasks;
let store: Store;
let indexer: Indexer;
let snapshots: Snapshots;
let retrieval: Retrieval;
let vectors: QdrantIndex;
let cmdSeq = 0;
let activeSpace = space;
/** Hooks around the real Qdrant query: the last body sent and an action after the answer. */
const spy: { last?: HybridQuery; after?: () => Promise<void> } = {};

const baseProfile: RetrievalProfile = {
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
let profile = baseProfile;

const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const bob: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "bob" };
const mallory: Scope = { tenantId: "tenant_b", projectId: "proj_b", actorId: "mallory" };

const cmd = (scope: Scope, digest = `sha256:${"c".repeat(64)}`): Command => ({
	tenantId: scope.tenantId,
	commandId: `snap-cmd-${++cmdSeq}`,
	actorId: scope.actorId,
	requestDigest: digest,
});

beforeAll(async () => {
	[inst, qdrant] = await Promise.all([startInstance(), startQdrant()]);
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	vectors = qdrantIndex(qdrant.url);
	const spied = new Proxy(vectors, {
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
	indexer = new Indexer(
		store,
		tasks,
		() => vectors,
		() => inference,
		() => objects,
		() => space,
		() => ({ batchSize: 8, maxResultBytes: 1 << 20, pollMs: 0 }),
		clock,
		silentLogger,
		metrics,
	);
	tasks.setRecords("knowledge-project", indexer);
	snapshots = new Snapshots(store, silentLogger);
	retrieval = new Retrieval(
		store,
		() => spied,
		() => inference,
		() => objects,
		() => activeSpace,
		() => profile,
		() => 1 << 20,
		clock,
		silentLogger,
		metrics,
	);
	for (let i = 0; i < 3; i++) await indexer.reconcile(); // generation 1 accepted
	const d = { indexer, tasks, store, clock };
	// tenant_a: a shared brand guide, alice's private notes; tenant_b: a secret.
	await indexSeeded(
		d,
		await seedParsed(inst, objects, {
			sourceId: "src-brand",
			tenantId: "tenant_a",
			texts: [
				"The hero banner uses teal backgrounds with slate headings.",
				"Buttons are rounded with eight pixel radius.",
				"Typography pairs Inter for body text with Lora for headings.",
			],
		}),
		"tenant_a",
	);
	await indexSeeded(
		d,
		await seedParsed(inst, objects, {
			sourceId: "src-notes",
			tenantId: "tenant_a",
			access: [{ principalType: "actor", principalId: "alice" }],
			texts: ["Alice private roadmap mentions the zyxqv launch codename."],
		}),
		"tenant_a",
	);
	await indexSeeded(
		d,
		await seedParsed(inst, objects, {
			sourceId: "src-secret",
			tenantId: "tenant_b",
			projectId: "proj_b",
			texts: ["Tenant B secret: the hero banner uses crimson backgrounds zyxqv."],
		}),
		"tenant_b",
	);
	await seedParsed(inst, objects, { sourceId: "src-unindexed", tenantId: "tenant_a", texts: ["not indexed yet"] });
	await indexSeeded(
		d,
		await seedParsed(inst, objects, {
			sourceId: "src-dup",
			tenantId: "tenant_a",
			texts: ["Buttons are rounded with eight pixel radius.", "Cards use soft shadows and rounded corners."],
		}),
		"tenant_a",
	);
}, 240_000);

afterAll(async () => {
	await store?.pool.end();
	await inst?.stop();
	await qdrant?.container.stop();
});

describe("snapshots", () => {
	it("freezes exact readable, indexed revisions and the accepted generation; a retried command returns the same snapshot", async () => {
		const c = cmd(alice);
		const out = await snapshots.create(c, alice, ["src-notes", "src-brand", "src-brand"]);
		expect(out.existing).toBe(false);
		expect(out.snapshot.generation).toBe(1);
		expect(out.snapshot.sources.map((s) => `${s.sourceId}@${s.sourceRevision}`)).toEqual([
			"src-brand@1",
			"src-notes@1",
		]);
		expect(out.snapshot.contentDigest).toMatch(/^sha256:[0-9a-f]{64}$/);
		const again = await snapshots.create(c, alice, ["src-notes", "src-brand"]);
		expect(again.existing).toBe(true);
		expect(again.snapshot.snapshotId).toBe(out.snapshot.snapshotId);
		await expect(
			snapshots.create({ ...c, requestDigest: `sha256:${"d".repeat(64)}` }, alice, ["src-brand"]),
		).rejects.toMatchObject({ code: "COMMAND_CONFLICT" });
	});

	it("does not distinguish unreadable from missing sources and refuses revisions that are not indexed", async () => {
		// bob cannot read alice's notes; nobody in tenant_a can read tenant_b's source.
		await expect(snapshots.create(cmd(bob), bob, ["src-brand", "src-notes"])).rejects.toMatchObject({
			code: "NOT_FOUND",
		});
		await expect(snapshots.create(cmd(alice), alice, ["src-secret"])).rejects.toMatchObject({ code: "NOT_FOUND" });
		await expect(snapshots.create(cmd(alice), alice, ["src-missing"])).rejects.toMatchObject({ code: "NOT_FOUND" });
		await expect(snapshots.create(cmd(alice), alice, ["src-unindexed"])).rejects.toMatchObject({
			code: "NOT_INDEXED",
		});
		// A command in another actor's name is refused before anything is read.
		await expect(snapshots.create({ ...cmd(alice), actorId: "bob" }, alice, ["src-brand"])).rejects.toBeInstanceOf(
			RetrievalError,
		);
	});

	it("reads a snapshot only in its tenant and lists only what the reader can read now, keeping the frozen digest", async () => {
		const out = await snapshots.create(cmd(alice), alice, ["src-brand", "src-notes"]);
		await expect(snapshots.get(mallory, out.snapshot.snapshotId)).rejects.toMatchObject({ code: "NOT_FOUND" });
		const asBob = await snapshots.get(bob, out.snapshot.snapshotId);
		expect(asBob.sources.map((s) => s.sourceId)).toEqual(["src-brand"]);
		expect(asBob.contentDigest).toBe(out.snapshot.contentDigest);
	});
});

/** Replaces a source's ACL with a new revision, as UpdateSourceAccess commits it. */
async function setAccess(sourceId: string, entries: [string, string][]): Promise<void> {
	const r = await inst.admin("SELECT acl_revision FROM sources WHERE source_id = $1", [sourceId]);
	const next = Number(r.rows[0]?.acl_revision) + 1;
	await inst.admin(
		"INSERT INTO source_acl_revisions (source_id, acl_revision, actor_id, command_id) VALUES ($1, $2, 'alice', $3)",
		[sourceId, next, `acl-${sourceId}-${next}`],
	);
	for (const [t, id] of entries)
		await inst.admin(
			"INSERT INTO source_acl (source_id, acl_revision, principal_type, principal_id) VALUES ($1, $2, $3, $4)",
			[sourceId, next, t, id],
		);
	await inst.admin("UPDATE sources SET acl_revision = $2 WHERE source_id = $1", [sourceId, next]);
}

const search = (scope: Scope, snapshotId: string, query: string, over: Partial<SearchRequest> = {}) =>
	retrieval.search({
		scope,
		snapshotId,
		query,
		maxContextItems: 8,
		retrievalProfileId: baseProfile.profileId,
		deadline: new Date(clock.now().getTime() + 30_000),
		...over,
	});

describe("hybrid retrieval", () => {
	it("answers with numbered, verifiable citations from the readable sources of the snapshot only", async () => {
		const snap = (await snapshots.create(cmd(alice), alice, ["src-brand", "src-notes"])).snapshot;
		const out = await search(alice, snap.snapshotId, "hero banner teal backgrounds");
		expect(out.noAnswer).toBe(false);
		expect(out.indexGeneration).toBe(1);
		expect(out.authorizationRevision).toMatch(/^sha256:[0-9a-f]{64}$/);
		const first = out.items[0];
		expect(first?.citation).toMatchObject({ ordinal: "1", sourceId: "src-brand", sourceRevision: "1" });
		expect(first?.text).toBe("The hero banner uses teal backgrounds with slate headings.");
		expect(first?.citation.score).toMatch(/^-?[0-9]+\.[0-9]{6}$/);
		expect(JSON.parse(first?.citation.locator ?? "{}")).toMatchObject({ headingPath: ["Doc"] });
		expect(out.items.map((i) => i.citation.ordinal)).toEqual(out.items.map((_, n) => String(n + 1)));
		expect(out.items.map((i) => i.text).join(" ")).not.toMatch(/crimson/);
		// Both prefetch branches and the outer query carry the allowed-source filter.
		expect(spy.last?.prefetch.map((p) => p.using).sort()).toEqual(["dense", "sparse"]);
		for (const f of [...(spy.last?.prefetch.map((p) => p.filter) ?? []), spy.last?.filter])
			expect(f?.must).toEqual(
				expect.arrayContaining([
					{ key: "tenant_id", match: { value: "tenant_a" } },
					{ key: "source_key", match: { any: ["src-brand@1", "src-notes@1"] } },
				]),
			);
	});

	it("never leaks a lexical-only match of an unauthorized source through the sparse branch", async () => {
		// The control: without the filter, the sparse branch alone finds the other tenant's and alice's private chunks.
		const raw = new QdrantClient({ url: qdrant.url, apiKey, checkCompatibility: false });
		const q = await inference.embed({}, "query", ["zyxqv"]);
		const unfiltered = await raw.query(collectionOf(1), {
			query: q.sparse[0] as { indices: number[]; values: number[] },
			using: "sparse",
			with_payload: true,
			limit: 10,
		});
		expect(unfiltered.points.map((p) => p.payload?.source_id).sort()).toEqual(["src-notes", "src-secret"]);
		// bob reads alice's snapshot (same tenant and project) but not her notes.
		const snap = (await snapshots.create(cmd(alice), alice, ["src-brand", "src-notes"])).snapshot;
		const before = inference.rerankTexts.length;
		const out = await search(bob, snap.snapshotId, "zyxqv");
		expect(out.items.map((i) => i.citation.sourceId)).not.toContain("src-notes");
		expect(JSON.stringify(out)).not.toMatch(/zyxqv|roadmap|crimson/);
		for (const texts of inference.rerankTexts.slice(before)) expect(texts.join(" ")).not.toMatch(/zyxqv/);
		for (const p of spy.last?.prefetch ?? [])
			expect(p.filter.must).toContainEqual({ key: "source_key", match: { any: ["src-brand@1"] } });
	});

	it("applies a revocation to a frozen snapshot on the next search without disclosing content or counts", async () => {
		const s = await seedParsed(inst, objects, {
			sourceId: "src-revoked",
			tenantId: "tenant_a",
			texts: ["Palette tokens: ocean, sand and moss."],
		});
		await indexSeeded({ indexer, tasks, store, clock }, s, "tenant_a");
		const snap = (await snapshots.create(cmd(alice), alice, ["src-revoked"])).snapshot;
		expect((await search(alice, snap.snapshotId, "palette ocean sand")).items).toHaveLength(1);
		await setAccess("src-revoked", [["actor", "bob"]]);
		const out = await search(alice, snap.snapshotId, "palette ocean sand");
		expect(out).toMatchObject({ items: [], noAnswer: true });
		expect(JSON.stringify(out)).not.toMatch(/ocean/);
		expect((await snapshots.get(alice, snap.snapshotId)).sources).toEqual([]);
	});

	it("rechecks authorization before any text is read and again before return", async () => {
		const s = await seedParsed(inst, objects, {
			sourceId: "src-race",
			tenantId: "tenant_a",
			texts: ["Spacing scale doubles from four pixels."],
		});
		await indexSeeded({ indexer, tasks, store, clock }, s, "tenant_a");
		const snap = (await snapshots.create(cmd(alice), alice, ["src-race", "src-brand"])).snapshot;
		// Revoked after the index answered: the text never reaches the reranker.
		spy.after = () => setAccess("src-race", [["actor", "bob"]]);
		const before = inference.rerankTexts.length;
		const out = await search(alice, snap.snapshotId, "spacing scale pixels");
		spy.after = undefined;
		expect(out.items.map((i) => i.citation.sourceId)).not.toContain("src-race");
		for (const texts of inference.rerankTexts.slice(before)) expect(texts.join(" ")).not.toMatch(/Spacing/);
		// Revoked while the reranker runs: the reranked text is withheld from the answer.
		await setAccess("src-race", [["tenant", "tenant_a"]]);
		inference.onRerank = () => setAccess("src-race", [["actor", "bob"]]);
		const during = await search(alice, snap.snapshotId, "spacing scale pixels");
		inference.onRerank = undefined;
		expect(during.items.map((i) => i.citation.sourceId)).not.toContain("src-race");
		expect(JSON.stringify(during)).not.toMatch(/Spacing/);
	});

	it("refuses an oversized allowed set instead of dropping ACL entries", async () => {
		const snap = (await snapshots.create(cmd(alice), alice, ["src-brand", "src-notes"])).snapshot;
		profile = { ...baseProfile, maxAllowedSources: 1 };
		await expect(search(alice, snap.snapshotId, "hero")).rejects.toMatchObject({ code: "SCOPE_TOO_LARGE" });
		profile = baseProfile;
	});

	it("refuses unknown profiles, passed deadlines, other tenants' snapshots and another model's vector space", async () => {
		const snap = (await snapshots.create(cmd(alice), alice, ["src-brand"])).snapshot;
		await expect(search(alice, snap.snapshotId, "hero", { retrievalProfileId: "hybrid-v0" })).rejects.toMatchObject({
			code: "PROFILE_UNQUALIFIED",
		});
		await expect(search(alice, snap.snapshotId, "hero", { deadline: clock.now() })).rejects.toMatchObject({
			code: "DEADLINE_EXCEEDED",
		});
		await expect(search(mallory, snap.snapshotId, "hero")).rejects.toMatchObject({ code: "NOT_FOUND" });
		activeSpace = { ...space, modelRevision: "e5-large-v2-revision" };
		await expect(search(alice, snap.snapshotId, "hero")).rejects.toBeInstanceOf(RetrievalError);
		await expect(search(alice, snap.snapshotId, "hero")).rejects.toMatchObject({ code: "PROFILE_UNQUALIFIED" });
		activeSpace = space;
	});

	it("bounds the context, drops duplicate content and answers no evidence instead of inventing citations", async () => {
		const snap = (await snapshots.create(cmd(alice), alice, ["src-brand", "src-dup"])).snapshot;
		const all = await search(alice, snap.snapshotId, "buttons rounded radius");
		const texts = all.items.map((i) => i.text);
		expect(texts.filter((t) => t === "Buttons are rounded with eight pixel radius.")).toHaveLength(1);
		expect((await search(alice, snap.snapshotId, "buttons rounded radius", { maxContextItems: 1 })).items).toHaveLength(
			1,
		);
		profile = { ...baseProfile, maxContextChars: 50 };
		const small = await search(alice, snap.snapshotId, "buttons rounded radius");
		expect(small.items.reduce((n, i) => n + Array.from(i.text).length, 0)).toBeLessThanOrEqual(50);
		profile = baseProfile;
		const none = await search(alice, snap.snapshotId, "quantum chromodynamics lattice");
		expect(none).toMatchObject({ items: [], noAnswer: true });
	});
});
