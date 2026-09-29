import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	InferenceClient,
	InferenceError,
	InferenceMismatch,
	type InferenceProfile,
	inputDigest,
} from "../src/adapters/inference.js";

const profile: InferenceProfile = {
	embeddingProfileId: "bge-m3-v1",
	embeddingModelRevision: "5617a9f61b028005a4858fdac845db406aefb181",
	dimensions: 4,
	rerankProfileId: "bge-reranker-v2-m3-v1",
	rerankModelRevision: "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e",
};

type Reply = (req: Record<string, unknown>) => { status: number; body: string };
let reply: Reply;
let server: Server;
let url: string;

beforeAll(async () => {
	server = createServer((req, res) => {
		const parts: Buffer[] = [];
		req.on("data", (c: Buffer) => parts.push(c));
		req.on("end", () => {
			const r = reply(JSON.parse(Buffer.concat(parts).toString("utf8")));
			res.writeHead(r.status, { "Content-Type": "application/json" });
			res.end(r.body);
		});
	});
	await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
	url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(() => new Promise<void>((r) => server.close(() => r())));

const ok = (body: unknown) => ({ status: 200, body: JSON.stringify(body) });

function embedAnswer(req: Record<string, unknown>, over: Record<string, unknown> = {}) {
	const n = (req.inputs as string[]).length;
	return ok({
		modelId: "bge-m3",
		modelRevision: profile.embeddingModelRevision,
		dimensions: 4,
		inputDigest: req.inputDigest,
		dense: Array.from({ length: n }, () => [0.5, 0.5, 0.5, 0.5]),
		sparse: Array.from({ length: n }, () => ({ indices: [2, 9], values: [0.3, 0.1] })),
		...over,
	});
}

describe("the contract's input digest", () => {
	it("agrees with the Inference service's Python vectors", () => {
		expect(inputDigest(["bge-m3-v1", "query", "Brand colors?", "Teal — é 雪"])).toBe(
			"sha256:ad8549a0ed7828ae7df38176852c34b7354cd584ac6e1df9c67e1817b1ec09f1",
		);
		expect(inputDigest(["bge-reranker-v2-m3-v1", "colors?", "c1", "Teal and slate.", "c2", "Inter"])).toBe(
			"sha256:b0d329006c5de2e239ced3dc64b56387241e3358824504e5e2bbfa98ecaa3ee9",
		);
	});
});

describe("Inference client", () => {
	const client = () => new InferenceClient(url, 5000, profile, 1 << 20);
	const compute = { taskId: "query-1", generation: "1" };

	it("sends the compute identity and digest and accepts an answer bound to them", async () => {
		let seen: Record<string, unknown> = {};
		reply = (req) => {
			seen = req;
			return embedAnswer(req);
		};
		const out = await client().embed(compute, "passage", ["a", "b"]);
		expect(out.dense).toHaveLength(2);
		expect(seen.compute).toEqual({ taskId: "query-1", generation: "1", profileId: "bge-m3-v1" });
		expect(seen.inputDigest).toBe(inputDigest(["bge-m3-v1", "passage", "a", "b"]));
		expect(Object.keys(seen).sort()).toEqual(["compute", "inputDigest", "inputKind", "inputs"]);
	});

	it("refuses stale or mismatched answers", async () => {
		const cases: [string, Reply][] = [
			["another revision", (r) => embedAnswer(r, { modelRevision: "0000000" })],
			["another digest", (r) => embedAnswer(r, { inputDigest: `sha256:${"0".repeat(64)}` })],
			["another dimension", (r) => embedAnswer(r, { dimensions: 1024 })],
			["missing vector", (r) => embedAnswer(r, { dense: [[0.5, 0.5, 0.5, 0.5]] })],
			[
				"unsorted sparse",
				(r) =>
					embedAnswer(r, {
						sparse: [
							{ indices: [9, 2], values: [1, 1] },
							{ indices: [], values: [] },
						],
					}),
			],
			["duplicate keys", () => ({ status: 200, body: '{"modelId":"bge-m3","modelId":"bge-m3"}' })],
			["oversized", () => ({ status: 200, body: JSON.stringify({ pad: "x".repeat(2 << 20) }) })],
		];
		for (const [name, r] of cases) {
			reply = r;
			await expect(client().embed(compute, "query", ["a", "b"]), name).rejects.toBeInstanceOf(InferenceMismatch);
		}
	});

	it("binds rerank scores to exactly the asked candidates", async () => {
		const cands = [
			{ candidateId: "c1", text: "Teal and slate." },
			{ candidateId: "c2", text: "Inter" },
		];
		const answer = (req: Record<string, unknown>, ids = ["c1", "c2"], revision = profile.rerankModelRevision) =>
			ok({
				modelId: "bge-reranker-v2-m3",
				modelRevision: revision,
				inputDigest: req.inputDigest,
				scores: ids.map((candidateId, i) => ({ candidateId, score: 2 - i })),
			});
		reply = (r) => answer(r);
		const scores = await client().rerank(compute, "colors?", cands);
		expect([...scores.entries()]).toEqual([
			["c1", 2],
			["c2", 1],
		]);
		reply = (r) => answer(r, ["c1", "c3"]);
		await expect(client().rerank(compute, "colors?", cands)).rejects.toBeInstanceOf(InferenceMismatch);
		reply = (r) => answer(r, ["c1", "c2"], "stale");
		await expect(client().rerank(compute, "colors?", cands)).rejects.toBeInstanceOf(InferenceMismatch);
	});

	it("maps the error envelope to its code and retryability", async () => {
		reply = () => ({
			status: 503,
			body: JSON.stringify({
				error: { code: "CAPACITY_EXHAUSTED", message: "full", requestId: "req-1", retryable: true },
			}),
		});
		const err = await client()
			.embed(compute, "query", ["a"])
			.catch((e: unknown) => e);
		expect(err).toBeInstanceOf(InferenceError);
		expect((err as InferenceError).code).toBe("CAPACITY_EXHAUSTED");
		expect((err as InferenceError).retryable).toBe(true);
		for (const [status, retryable, code] of [
			[503, true, "DEPENDENCY_UNAVAILABLE"],
			[502, true, "DEPENDENCY_UNAVAILABLE"],
			[400, false, "INVALID_ARGUMENT"],
		] as const) {
			reply = () => ({ status, body: "Service Unavailable" });
			const plain = await client()
				.embed(compute, "query", ["a"])
				.catch((e: unknown) => e);
			expect(plain, `plain-text ${status}`).toBeInstanceOf(InferenceError);
			expect((plain as InferenceError).retryable).toBe(retryable);
			expect((plain as InferenceError).code).toBe(code);
		}
		const down = new InferenceClient("http://127.0.0.1:1", 1000, profile);
		await expect(down.embed(compute, "query", ["a"])).rejects.toMatchObject({ code: "DEPENDENCY_UNAVAILABLE" });
	});
});

const liveUrl = process.env.ANVILKIT_KNOWLEDGE_TEST_INFERENCE_URL;
describe.skipIf(!liveUrl)("against a running anvilkit-agent-inference (ANVILKIT_KNOWLEDGE_TEST_INFERENCE_URL)", () => {
	it("embeds and reranks under the locked profile", async () => {
		const c = new InferenceClient(liveUrl ?? "", 120_000, { ...profile, dimensions: 1024 });
		const e = await c.embed({ taskId: "live-1", generation: "1" }, "passage", [
			"What are the brand colors?",
			"Brand colors are teal and slate.",
			"Headings use Inter.",
		]);
		const dot = (a: number[], b: number[]) => a.reduce((s, x, i) => s + x * (b[i] ?? 0), 0);
		expect(dot(e.dense[0] ?? [], e.dense[1] ?? [])).toBeGreaterThan(dot(e.dense[0] ?? [], e.dense[2] ?? []));
		const s = await c.rerank({ taskId: "live-1", generation: "1" }, "What are the brand colors?", [
			{ candidateId: "c1", text: "Headings use Inter." },
			{ candidateId: "c2", text: "Brand colors are teal and slate." },
		]);
		expect(s.get("c2") ?? 0).toBeGreaterThan(s.get("c1") ?? 0);
		const stale = new InferenceClient(liveUrl ?? "", 120_000, {
			...profile,
			dimensions: 1024,
			embeddingModelRevision: "stale",
		});
		await expect(stale.embed({ taskId: "live-2", generation: "1" }, "query", ["x"])).rejects.toBeInstanceOf(
			InferenceMismatch,
		);
	}, 300_000);
});
