// P2.7 (AC6): an embedding-model change on real PostgreSQL 17 and Qdrant
// 1.19.0. Generation 1 is accepted under model m1; the reviewed profile
// switches to m2 (a second Inference deployment, the doubles here) while
// m1's deployment stays placed as the retained placement. Generation 2 is
// built with m2 while Search, CreateSnapshot and RecallFacts keep serving
// generation 1 with m1 query embeddings (new sources and facts included);
// once generation 2 is completely re-ingested and accepted, queries use m2
// on it. Vectors of one model are never written into or compared with
// another model's generation, and a reverted change never accepts its
// abandoned generation. The doubles validate the mechanics, never quality.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { embedderFor } from "../src/adapters/inference.js";
import type { HybridQuery, ScoredPoint, VectorIndex } from "../src/adapters/qdrant.js";
import { Recall } from "../src/application/recall.js";
import { Retrieval } from "../src/application/retrieval.js";
import { Snapshots } from "../src/application/snapshots.js";
import { collectionOf, indexTaskIdOf, type SpaceProfile, vectorsFit } from "../src/domain/index.js";
import type { RetrievalProfile } from "../src/domain/retrieval.js";
import type { Scope } from "../src/domain/source.js";
import { silentLogger } from "../src/log.js";
import { acceptGeneration, drainProjections, type Kit, startKit } from "./memorykit.js";
import { HashInference, indexSeeded, seedParsed, space } from "./rag.js";

const m1: SpaceProfile = space;
const m2: SpaceProfile = {
	...space,
	modelRevision: "bge-m3-next-revision",
	embeddingProfile: "bge-m3-v2",
	sparseProfile: "bge-m3-lexical-v2",
};
const model2 = new HashInference({ revision: m2.modelRevision, dimensions: 1024, salt: "m2:" });
const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const agent: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "agent-coder" };
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

let k: Kit;
let model1: HashInference;
let snapshots: Snapshots;
let retrieval: Retrieval;
let recall: Recall;
let searched = 0;
let seq = 0;
let s1 = "";
let darkFact = "";
let paletteFact = "";

const search = (snapshotId: string, query: string) =>
	retrieval.search({
		scope: alice,
		snapshotId,
		query,
		maxContextItems: 8,
		retrievalProfileId: profile.profileId,
		deadline: new Date(k.clock.now().getTime() + 30_000),
	});

const ask = (query: string) =>
	recall.recall({
		scope: alice,
		query,
		maxFacts: 5,
		retrievalProfileId: profile.profileId,
		deadline: new Date(k.clock.now().getTime() + 30_000),
	});

const snapshot = async (sourceIds: string[]) => (await snapshots.create(k.cmd(alice), alice, sourceIds)).snapshot;

async function code(p: Promise<unknown>): Promise<string> {
	try {
		await p;
		return "OK";
	} catch (err) {
		return (err as { code?: string }).code ?? String(err);
	}
}

async function state(generation: number): Promise<string | undefined> {
	return (await k.inst.admin("SELECT state FROM index_generations WHERE generation = $1", [generation])).rows[0]?.state;
}

async function points(generation: number, sourceId: string): Promise<number> {
	return k.vectors.count(collectionOf(generation), { must: [{ key: "source_id", match: { value: sourceId } }] });
}

/** Claims, advances and submits one index entry like the Background Worker; answers its outcome. */
async function drive(sourceId: string, generation: number): Promise<string> {
	const taskId = indexTaskIdOf(sourceId, 1, generation);
	const workerId = `mc-worker-${++seq}`;
	const claimed = await k.tasks.claim(taskId, 1, workerId, 600_000);
	for (let i = 0; i < 100; i++) {
		const a = await k.indexer.advance(taskId, 1, workerId, claimed.task.inputDigest);
		if (a.state === "running") continue;
		if (a.state === "failed") return a.failureCode;
		const r = await k.tasks.submit(taskId, 1, {
			workerId,
			inputDigest: claimed.task.inputDigest,
			succeeded: true,
			resultRef: a.resultRef,
			resultDigest: a.resultDigest,
			failureCode: "",
		});
		return r.accepted ? "accepted" : "refused";
	}
	throw new Error(`${taskId} did not settle`);
}

/** A parsed source scheduled into every open generation of its chunker (as an accepted parse does). */
async function arrive(sourceId: string, text: string): Promise<number> {
	const s = await seedParsed(k.inst, k.objects, { sourceId, tenantId: "tenant_a", texts: [text] });
	return k.store.inTx((c) =>
		k.indexer.scheduleIn(c, {
			requestId: s.requestId,
			sourceId,
			sourceRevision: 1,
			tenantId: "tenant_a",
			chunkerProfile: m1.chunkerProfile,
			chunkerRevision: m1.chunkerRevision,
			chunkCount: 1,
			correlationId: "model-change",
		}),
	);
}

async function confirmFact(content: string): Promise<string> {
	const p = await k.memory.propose(k.cmd(agent), agent, {
		subjectType: "actor",
		subjectId: "alice",
		content,
		sourceRefs: [],
		expiresAt: null,
		origin: "model",
	});
	return (await k.memory.decide(k.cmd(alice), alice, p.fact.factId, 1, "confirm", "", null)).fact.factId;
}

beforeAll(async () => {
	k = await startKit();
	model1 = k.inference;
	await acceptGeneration(k);
	const counted = new Proxy(k.vectors, {
		get: (target, prop, recv) =>
			prop === "search"
				? async (collection: string, q: HybridQuery): Promise<ScoredPoint[]> => {
						searched++;
						return target.search(collection, q);
					}
				: Reflect.get(target, prop, recv),
	}) as VectorIndex;
	snapshots = new Snapshots(k.store, silentLogger);
	retrieval = new Retrieval(
		k.store,
		() => counted,
		() => k.activeInference.current,
		() => k.objects,
		() => k.activeSpace.current,
		() => profile,
		() => 1 << 20,
		k.clock,
		silentLogger,
		k.metrics,
	);
	retrieval.setRetained(() => k.retained.current);
	recall = new Recall(
		k.store,
		() => counted,
		() => k.activeInference.current,
		() => k.activeSpace.current,
		() => profile,
		() => k.bounds,
		k.clock,
		silentLogger,
		k.metrics,
	);
	recall.setRetained(() => k.retained.current);
	const d = { indexer: k.indexer, tasks: k.tasks, store: k.store, clock: k.clock };
	for (const [id, text] of <[string, string][]>[
		["src-hero", "Hero banners use teal gradients."],
		["src-footer", "Footers list legal links in slate."],
	])
		await indexSeeded(
			d,
			await seedParsed(k.inst, k.objects, { sourceId: id, tenantId: "tenant_a", texts: [text] }),
			"tenant_a",
		);
	darkFact = await confirmFact("alice prefers dark mode previews");
	await drainProjections(k);
	s1 = (await snapshot(["src-hero", "src-footer"])).snapshotId;
}, 240_000);

afterAll(async () => {
	await k?.stop();
});

describe("embedding-model change (P2.7 AC6)", () => {
	it("before the change, generation 1 answers with m1", async () => {
		const q = model1.queries;
		const r = await search(s1, "teal hero banners");
		expect(r.indexGeneration).toBe(1);
		expect(r.items[0]?.citation.sourceId).toBe("src-hero");
		expect(model1.queries - q).toBe(1);
	});

	it("builds generation 2 with m2 while search, snapshots and recall keep serving generation 1 with m1", async () => {
		// The reviewed profile switches to m2; m1's deployment is the retained placement.
		k.activeSpace.current = m2;
		k.activeInference.current = model2;
		k.retained.current = { space: m1, inference: model1 };
		await k.indexer.reconcile(); // generation 2 of m2 (building), its collection and backfill
		await k.projector.reconcile(); // memory points of the confirmed fact into generation 2
		await drainProjections(k);
		expect(await state(1)).toBe("accepted");
		expect(await state(2)).toBe("building");
		const g2 = await k.inst.admin(
			"SELECT model_revision, embedding_profile, sparse_profile FROM index_generations WHERE generation = 2",
		);
		expect(g2.rows[0]).toEqual({
			model_revision: m2.modelRevision,
			embedding_profile: m2.embeddingProfile,
			sparse_profile: m2.sparseProfile,
		});
		expect(await k.vectors.aliasTarget()).toBe(collectionOf(1));

		// Search on the accepted generation: m1 embeds the query, m2 never does.
		const [q1, q2] = [model1.queries, model2.queries];
		const r = await search(s1, "teal hero banners");
		expect(r.indexGeneration).toBe(1);
		expect(r.items[0]?.citation.sourceId).toBe("src-hero");
		expect([model1.queries - q1, model2.queries - q2]).toEqual([1, 0]);

		// A snapshot of previously indexed sources is never NOT_INDEXED during the switch.
		const during = await snapshot(["src-hero", "src-footer"]);
		expect(during.generation).toBe(1);
		expect((await search(during.snapshotId, "footers legal links slate")).items[0]?.citation.sourceId).toBe(
			"src-footer",
		);

		// Recall reads the accepted generation's memory points with m1.
		const [r1, r2] = [model1.queries, model2.queries];
		const recalled = await ask("dark mode previews");
		expect(recalled.indexGeneration).toBe(1);
		expect(recalled.facts.map((f) => f.factId)).toContain(darkFact);
		expect([model1.queries - r1, model2.queries - r2]).toEqual([1, 0]);

		// A source arriving during the switch is written into both generations, each with its own model.
		expect(await arrive("src-cards", "Cards use slate borders with teal accents.")).toBe(2);
		const [p1, p2] = [model1.passages, model2.passages];
		expect(await drive("src-cards", 1)).toBe("accepted");
		expect([model1.passages - p1, model2.passages - p2]).toEqual([1, 0]);
		expect(await drive("src-cards", 2)).toBe("accepted");
		expect(model2.passages - p2).toBe(1);
		const cards = await snapshot(["src-cards"]);
		expect(cards.generation).toBe(1);
		expect((await search(cards.snapshotId, "cards slate borders")).items[0]?.citation.sourceId).toBe("src-cards");

		// A fact confirmed during the switch is projected into both and recalled from generation 1.
		paletteFact = await confirmFact("alice uses the brand palette in reviews");
		await drainProjections(k);
		const palette = await ask("brand palette reviews");
		expect(palette.indexGeneration).toBe(1);
		expect(palette.facts.map((f) => f.factId)).toContain(paletteFact);

		// Partly re-ingested: generation 2 does not qualify and the alias stays.
		expect(await drive("src-hero", 2)).toBe("accepted");
		for (let i = 0; i < 3; i++) await k.indexer.reconcile();
		expect(await state(2)).toBe("building");
		expect(await state(1)).toBe("accepted");
		expect(await k.vectors.aliasTarget()).toBe(collectionOf(1));
		expect((await search(s1, "teal hero banners")).indexGeneration).toBe(1);
	});

	it("never mixes model spaces: another model's vectors are refused before Qdrant is asked or written", async () => {
		// A misrouted placement (m2's deployment named for m1's space) answers another model revision.
		k.retained.current = { space: m1, inference: model2 };
		const before = searched;
		expect(await code(search(s1, "teal hero banners"))).toBe("PROFILE_UNQUALIFIED");
		expect(await code(ask("dark mode previews"))).toBe("PROFILE_UNQUALIFIED");
		// Equal revision, other dimensions: refused as well.
		k.retained.current = {
			space: m1,
			inference: new HashInference({ revision: m1.modelRevision, dimensions: 768, salt: "" }),
		};
		expect(await code(search(s1, "teal hero banners"))).toBe("PROFILE_UNQUALIFIED");
		// Without a placement of m1's space, generation 1 is refused, never queried with m2.
		k.retained.current = undefined;
		const q2 = model2.queries;
		expect(await code(search(s1, "teal hero banners"))).toBe("PROFILE_UNQUALIFIED");
		expect(await code(ask("dark mode previews"))).toBe("PROFILE_UNQUALIFIED");
		expect(model2.queries).toBe(q2);
		expect(searched).toBe(before);

		// The index side: an entry of generation 1 embedded by another model fails and writes nothing.
		k.retained.current = { space: m1, inference: model2 };
		expect(await arrive("src-grid", "Grids align to an eight column layout.")).toBe(2);
		expect(await drive("src-grid", 1)).toBe("PROFILE_UNQUALIFIED");
		expect(await points(1, "src-grid")).toBe(0);
		k.retained.current = { space: m1, inference: model1 };
		expect(await drive("src-grid", 2)).toBe("accepted");
		expect(await points(2, "src-grid")).toBe(1);

		// The selection rule itself.
		const placements = [
			{ space: m2, inference: model2 },
			{ space: m1, inference: model1 },
		];
		expect(embedderFor(m1, placements)).toBe(model1);
		expect(embedderFor(m2, placements)).toBe(model2);
		expect(embedderFor({ ...m1, dimensions: 768 }, placements)).toBeUndefined();
		expect(vectorsFit(m1, { modelRevision: m1.modelRevision, dense: [new Array(1024).fill(0)] })).toBe(true);
		expect(vectorsFit(m1, { modelRevision: m2.modelRevision, dense: [new Array(1024).fill(0)] })).toBe(false);
		expect(vectorsFit(m1, { modelRevision: m1.modelRevision, dense: [new Array(768).fill(0)] })).toBe(false);
	});

	it("accepts generation 2 after the complete re-ingest; queries then use m2 on it", async () => {
		expect(await drive("src-footer", 2)).toBe("accepted");
		await drainProjections(k);
		for (let i = 0; i < 3 && (await state(2)) !== "accepted"; i++) await k.indexer.reconcile();
		expect(await state(2)).toBe("accepted");
		expect(await state(1)).toBe("retired");
		expect(await k.vectors.aliasTarget()).toBe(collectionOf(2));
		for (const id of ["src-hero", "src-footer", "src-cards", "src-grid"]) expect(await points(2, id)).toBe(1);

		const fresh = await snapshot(["src-hero", "src-footer", "src-cards"]);
		expect(fresh.generation).toBe(2);
		const [q1, q2] = [model1.queries, model2.queries];
		const r = await search(fresh.snapshotId, "teal hero banners");
		expect(r.indexGeneration).toBe(2);
		expect(r.items[0]?.citation.sourceId).toBe("src-hero");
		const recalled = await ask("brand palette reviews");
		expect(recalled.indexGeneration).toBe(2);
		expect(recalled.facts.map((f) => f.factId)).toContain(paletteFact);
		expect([model1.queries - q1, model2.queries - q2]).toEqual([0, 2]);

		// The old snapshot reads its retained generation with m1 while m1 stays placed, and is refused after.
		const old = await search(s1, "teal hero banners");
		expect(old.indexGeneration).toBe(1);
		expect(model1.queries - q1).toBe(1);
		k.retained.current = undefined;
		expect(await code(search(s1, "teal hero banners"))).toBe("PROFILE_UNQUALIFIED");
		expect(model2.queries - q2).toBe(2);
	});

	it("a reverted change never accepts its abandoned generation over the one that serves", async () => {
		const m3: SpaceProfile = { ...m2, modelRevision: "bge-m3-third-revision", embeddingProfile: "bge-m3-v3" };
		const model3 = new HashInference({ revision: m3.modelRevision, dimensions: 1024, salt: "m3:" });
		k.activeSpace.current = m3;
		k.activeInference.current = model3;
		k.retained.current = { space: m2, inference: model2 };
		await k.indexer.reconcile();
		await k.projector.reconcile();
		await drainProjections(k);
		expect(await state(3)).toBe("building");
		for (const id of ["src-hero", "src-footer", "src-cards", "src-grid"]) expect(await drive(id, 3)).toBe("accepted");
		// Reverted before generation 3 qualified: m2 is active again and nothing is retained.
		k.activeSpace.current = m2;
		k.activeInference.current = model2;
		k.retained.current = undefined;
		for (let i = 0; i < 3; i++) await k.indexer.reconcile();
		expect(await state(3)).toBe("building");
		expect(await state(2)).toBe("accepted");
		expect(await k.vectors.aliasTarget()).toBe(collectionOf(2));
		const r = await search((await snapshot(["src-hero"])).snapshotId, "teal hero banners");
		expect(r.indexGeneration).toBe(2);
	});
});
