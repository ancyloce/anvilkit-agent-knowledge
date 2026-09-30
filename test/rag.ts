// Shared fixtures of the P16 tests: a real Qdrant 1.19.0 node, a
// deterministic Inference double and a seeder that leaves a parsed source
// exactly as P15's acceptance does (source, revision, ACL revision, ingest
// request in state indexing, accepted chunks and the verified parser result
// object the chunk texts are read from). The double hashes words into a
// normalized 1024-d dense vector and a sparse term vector, so lexical and
// "semantic" overlap behave predictably; it validates the Index Builder and
// Retrieval mechanics, never retrieval quality (that is the evaluation run
// against the real BGE-M3 service, tests/evals/rag).
import { createHash } from "node:crypto";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { type Embedded, InferenceError, type InferencePort } from "../src/adapters/inference.js";
import type { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { QdrantIndex } from "../src/adapters/qdrant.js";
import type { Indexer } from "../src/application/indexer.js";
import type { Tasks } from "../src/application/tasks.js";
import type { SpaceProfile } from "../src/domain/index.js";
import { indexTaskIdOf } from "../src/domain/index.js";
import { chunkIdOf } from "../src/domain/parse.js";
import type { AccessEntry } from "../src/domain/source.js";
import type { FakeClock, Instance } from "./harness.js";

export const qdrantImage =
	"qdrant/qdrant:v1.19.0@sha256:057ee3a8da769fe7310dd3537b4dc7583bf87a95ce8ac43c0af5a46bc580d1fc";
export const apiKey = "test-qdrant-key";

export async function startQdrant(): Promise<{ container: StartedTestContainer; url: string }> {
	const container = await new GenericContainer(qdrantImage)
		.withEnvironment({ QDRANT__SERVICE__API_KEY: apiKey, QDRANT__TELEMETRY_DISABLED: "true" })
		.withExposedPorts(6333)
		.withWaitStrategy(Wait.forHttp("/readyz", 6333))
		.start();
	return { container, url: `http://${container.getHost()}:${container.getMappedPort(6333)}` };
}

export function qdrantIndex(url: string): QdrantIndex {
	return new QdrantIndex({
		url,
		apiKey,
		timeoutMs: 10_000,
		replicationFactor: 1,
		writeConsistencyFactor: 1,
		writeOrdering: "strong",
		readConsistency: "all",
	});
}

export const revision = "5617a9f61b028005a4858fdac845db406aefb181";

export const space: SpaceProfile = {
	modelId: "bge-m3",
	modelRevision: revision,
	embeddingProfile: "bge-m3-v1",
	dimensions: 1024,
	sparseProfile: "bge-m3-lexical-v1",
	chunkerProfile: "docling-hierarchical",
	chunkerRevision: 1,
};

const words = (t: string) => t.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [];
const bucket = (w: string, n: number) => createHash("sha256").update(w).digest().readUInt32BE(0) % n;

/** A deterministic stand-in for BGE-M3 (bag of hashed words) and the reranker (word overlap). */
export class HashInference implements InferencePort {
	embeds = 0;
	reranks = 0;
	rerankTexts: string[][] = [];
	failNext: "retryable" | "refused" | undefined;
	onRerank?: () => Promise<void>;

	async embed(_c: unknown, _kind: "query" | "passage", inputs: string[]): Promise<Embedded> {
		if (this.failNext) {
			const f = this.failNext;
			this.failNext = undefined;
			throw new InferenceError(f === "retryable" ? 503 : 400, "X", f === "retryable", "double");
		}
		this.embeds++;
		const dense: number[][] = [];
		const sparse: { indices: number[]; values: number[] }[] = [];
		for (const t of inputs) {
			const v = new Array<number>(1024).fill(0);
			const terms = new Map<number, number>();
			for (const w of words(t)) {
				v[bucket(w, 1024)] = (v[bucket(w, 1024)] ?? 0) + 1;
				const k = bucket(`s:${w}`, 250_002);
				terms.set(k, (terms.get(k) ?? 0) + 1);
			}
			const norm = Math.hypot(...v) || 1;
			dense.push(norm === 1 && v.every((x) => x === 0) ? v.map((_, i) => (i === 0 ? 1 : 0)) : v.map((x) => x / norm));
			const idx = [...terms.keys()].sort((a, b) => a - b);
			sparse.push({ indices: idx, values: idx.map((i) => terms.get(i) ?? 0) });
		}
		return { dense, sparse, modelRevision: revision };
	}

	async rerank(_c: unknown, query: string, candidates: { candidateId: string; text: string }[]) {
		this.reranks++;
		this.rerankTexts.push(candidates.map((c) => c.text));
		await this.onRerank?.();
		const q = new Set(words(query));
		return new Map(
			candidates.map((c) => {
				const w = words(c.text);
				const hits = w.filter((x) => q.has(x)).length;
				return [c.candidateId, w.length === 0 ? -10 : hits / Math.sqrt(w.length) - (hits === 0 ? 5 : 0)];
			}),
		);
	}
}

const sha = (s: string) => `sha256:${createHash("sha256").update(s).digest("hex")}`;

export interface Seeded {
	sourceId: string;
	requestId: string;
	chunkIds: string[];
	texts: string[];
}

/**
 * A parsed source as P15's acceptance leaves it. The ACL defaults to the
 * tenant; texts become chunks 0..n-1 of one parser result object.
 */
export async function seedParsed(
	inst: Instance,
	objects: MemoryObjects,
	p: { sourceId: string; tenantId: string; projectId?: string; access?: AccessEntry[]; texts: string[] },
): Promise<Seeded> {
	const projectId = p.projectId ?? "proj_a";
	const access = p.access ?? [{ principalType: "tenant", principalId: p.tenantId }];
	const requestId = `ing-${p.sourceId}-r1`;
	const launchKey = `parse-${createHash("sha256").update(p.sourceId).digest("hex").slice(0, 40)}`;
	const resultKey = `parse/${launchKey}/result.json`;
	await inst.admin(
		`INSERT INTO sources (source_id, tenant_id, project_id, kind, locator, current_revision, acl_revision, command_id, request_digest)
		 VALUES ($1, $2, $3, 'document', 'upload:x.md', 1, 1, $4, $5)`,
		[p.sourceId, p.tenantId, projectId, `cmd-${p.sourceId}`, sha(p.sourceId)],
	);
	await inst.admin(
		`INSERT INTO source_revisions (source_id, revision, content_digest, media_type, size_bytes, object_key)
		 VALUES ($1, 1, $2, 'text/markdown', 1, 'sources/x')`,
		[p.sourceId, sha(p.texts.join("\n"))],
	);
	await inst.admin(
		"INSERT INTO source_acl_revisions (source_id, acl_revision, actor_id, command_id) VALUES ($1, 1, 'alice', $2)",
		[p.sourceId, `cmd-${p.sourceId}`],
	);
	for (const e of access)
		await inst.admin(
			"INSERT INTO source_acl (source_id, acl_revision, principal_type, principal_id) VALUES ($1, 1, $2, $3)",
			[p.sourceId, e.principalType, e.principalId],
		);
	await inst.admin(
		`INSERT INTO ingest_requests (request_id, source_id, source_revision, state, parser_profile, parser_profile_revision,
		   chunker_profile, chunker_revision, task_id, chunk_count, page_count)
		 VALUES ($1, $2, 1, 'indexing', 'parser-docling-dev-v1', 1, 'docling-hierarchical', 1, $3, $4, 1)`,
		[requestId, p.sourceId, `ingest-${p.sourceId}-r1`, p.texts.length],
	);
	const chunkIds: string[] = [];
	const chunks = p.texts.map((text, ordinal) => ({
		ordinal,
		text,
		contentDigest: sha(text),
		locator: { element: "text", headingPath: ["Doc"], lineStart: ordinal + 1, lineEnd: ordinal + 1 },
		qualityFlags: [],
	}));
	for (const c of chunks) {
		const chunkId = chunkIdOf({
			sourceId: p.sourceId,
			sourceRevision: 1,
			parserProfile: "parser-docling-dev-v1",
			parserProfileRevision: 1,
			chunkerProfile: "docling-hierarchical",
			chunkerRevision: 1,
			ordinal: c.ordinal,
		});
		chunkIds.push(chunkId);
		await inst.admin(
			`INSERT INTO chunks (chunk_id, source_id, source_revision, parser_profile, parser_profile_revision, chunker_profile,
			   chunker_revision, ordinal, locator, content_digest, content_ref, ingest_request_id)
			 VALUES ($1, $2, 1, 'parser-docling-dev-v1', 1, 'docling-hierarchical', 1, $3, $4, $5, $6, $7)`,
			[
				chunkId,
				p.sourceId,
				c.ordinal,
				JSON.stringify(c.locator),
				c.contentDigest,
				`${resultKey}#${c.ordinal}`,
				requestId,
			],
		);
	}
	objects.objects.set(resultKey, Buffer.from(JSON.stringify({ schemaVersion: 1, launchKey, chunks })));
	return { sourceId: p.sourceId, requestId, chunkIds, texts: p.texts };
}

let workers = 0;

/** Schedules, claims, advances and submits the index entry of a seeded source in one generation. */
export async function indexSeeded(
	d: { indexer: Indexer; tasks: Tasks; store: Store; clock: FakeClock },
	s: Seeded,
	tenantId: string,
	generation = 1,
): Promise<void> {
	await d.store.inTx((c) =>
		d.indexer.scheduleIn(c, {
			requestId: s.requestId,
			sourceId: s.sourceId,
			sourceRevision: 1,
			tenantId,
			chunkerProfile: "docling-hierarchical",
			chunkerRevision: 1,
			chunkCount: s.texts.length,
			correlationId: "seed",
		}),
	);
	const taskId = indexTaskIdOf(s.sourceId, 1, generation);
	const workerId = `seed-${++workers}`;
	const claimed = await d.tasks.claim(taskId, 1, workerId, 600_000);
	for (let i = 0; i < 100; i++) {
		const a = await d.indexer.advance(taskId, 1, workerId, claimed.task.inputDigest);
		if (a.state === "running") continue;
		if (a.state === "failed") throw new Error(`index ${taskId}: ${a.failureCode}`);
		const r = await d.tasks.submit(taskId, 1, {
			workerId,
			inputDigest: claimed.task.inputDigest,
			succeeded: true,
			resultRef: a.resultRef,
			resultDigest: a.resultDigest,
			failureCode: "",
		});
		if (!r.accepted) throw new Error(`index ${taskId} not accepted`);
		return;
	}
	throw new Error(`index ${taskId} did not settle`);
}
