// Knowledge's client of anvilkit-agent-inference (contracts/openapi/
// inference.yaml; DD-07 §1): the only caller. Every request carries the
// task compute identity and the contract's input digest (length-prefixed
// UTF-8 parts); every answer is parsed strictly within a bound and must
// echo that digest and the model revision, dimensions and candidates the
// reviewed profile expects, or it is refused as stale. Inference is
// computation only: it never sees a tenant, ACL, source or storage key.
import { createHash } from "node:crypto";
import type { components } from "@anvilkit/generated-clients/openapi/inference";
import { parseStrictJson } from "@anvilkit/generated-clients/validation/json";

type EmbeddingRequest = components["schemas"]["EmbeddingRequest"];
type EmbeddingResponse = components["schemas"]["EmbeddingResponse"];
type RerankRequest = components["schemas"]["RerankRequest"];
type RerankResponse = components["schemas"]["RerankResponse"];
type ErrorEnvelope = components["schemas"]["ErrorEnvelope"];

/** The reviewed compute profile Knowledge binds its vectors and scores to. */
export interface InferenceProfile {
	embeddingProfileId: string;
	embeddingModelRevision: string;
	dimensions: number;
	rerankProfileId: string;
	rerankModelRevision: string;
}

export interface Compute {
	taskId: string;
	generation: string;
}

/** The contract's input digest: each part as its UTF-8 byte length, a line feed and the bytes. */
export function inputDigest(parts: string[]): string {
	const h = createHash("sha256");
	for (const p of parts) {
		const b = Buffer.from(p, "utf8");
		h.update(String(b.length));
		h.update("\n");
		h.update(b);
	}
	return `sha256:${h.digest("hex")}`;
}

/** An answer that does not bind to the request or the profile: never used. */
export class InferenceMismatch extends Error {}

/** A refusal or an outage, with the contract's code and whether a retry may help. */
export class InferenceError extends Error {
	constructor(
		readonly status: number,
		readonly code: string,
		readonly retryable: boolean,
		message: string,
	) {
		super(message);
	}
}

export interface Embedded {
	dense: number[][];
	sparse: { indices: number[]; values: number[] }[];
	modelRevision: string;
}

export class InferenceClient {
	constructor(
		private readonly url: string,
		private readonly timeoutMs: number,
		private readonly profile: InferenceProfile,
		private readonly maxResponseBytes = 64 << 20,
	) {}

	private async post(path: string, body: unknown): Promise<unknown> {
		let res: Response;
		try {
			res = await fetch(new URL(`/api/v1/${path}`, this.url), {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(body),
				redirect: "error",
				signal: AbortSignal.timeout(this.timeoutMs),
			});
		} catch (err) {
			throw new InferenceError(
				0,
				"DEPENDENCY_UNAVAILABLE",
				true,
				`inference unreachable: ${String(err).slice(0, 200)}`,
			);
		}
		const declared = Number(res.headers.get("content-length") ?? "0");
		if (declared > this.maxResponseBytes) throw new InferenceMismatch("response over the bound");
		const bytes = Buffer.from(await res.arrayBuffer());
		if (bytes.length > this.maxResponseBytes) throw new InferenceMismatch("response over the bound");
		if (!res.ok) {
			// An error is classified by its status first: an overloaded server
			// or a proxy may answer in plain text, which is an outage, never a
			// stale answer.
			let e: Partial<ErrorEnvelope["error"]> | undefined;
			try {
				e = (parseStrictJson(bytes.toString("utf8")) as Partial<ErrorEnvelope>)?.error;
			} catch {
				e = undefined;
			}
			const unavailable = res.status >= 500 || res.status === 429;
			throw new InferenceError(
				res.status,
				e?.code ?? (unavailable ? "DEPENDENCY_UNAVAILABLE" : "INVALID_ARGUMENT"),
				e?.retryable ?? unavailable,
				`inference ${res.status} ${e?.code ?? ""}`.trim(),
			);
		}
		let value: unknown;
		try {
			value = parseStrictJson(bytes.toString("utf8"));
		} catch {
			throw new InferenceMismatch("response is not strict JSON");
		}
		return value;
	}

	async embed(compute: Compute, kind: "query" | "passage", inputs: string[]): Promise<Embedded> {
		const p = this.profile;
		const req: EmbeddingRequest = {
			compute: { taskId: compute.taskId, generation: compute.generation, profileId: p.embeddingProfileId },
			inputKind: kind,
			inputs,
			inputDigest: inputDigest([p.embeddingProfileId, kind, ...inputs]),
		};
		const r = (await this.post("embeddings", req)) as EmbeddingResponse;
		if (r.inputDigest !== req.inputDigest) throw new InferenceMismatch("answer names another input digest");
		if (r.modelId !== "bge-m3" || r.modelRevision !== p.embeddingModelRevision)
			throw new InferenceMismatch(`model revision ${String(r.modelRevision)} is not the profile's`);
		if (r.dimensions !== p.dimensions) throw new InferenceMismatch(`dimensions ${String(r.dimensions)}`);
		if (
			!Array.isArray(r.dense) ||
			!Array.isArray(r.sparse) ||
			r.dense.length !== inputs.length ||
			r.sparse.length !== inputs.length
		)
			throw new InferenceMismatch("answer does not cover every input");
		for (const v of r.dense)
			if (!Array.isArray(v) || v.length !== p.dimensions || !v.every(Number.isFinite))
				throw new InferenceMismatch("dense vector shape");
		for (const s of r.sparse)
			if (
				s.indices.length !== s.values.length ||
				!s.indices.every((i, n) => Number.isSafeInteger(i) && i >= 0 && (n === 0 || i > (s.indices[n - 1] ?? -1))) ||
				!s.values.every(Number.isFinite)
			)
				throw new InferenceMismatch("sparse vector shape");
		return { dense: r.dense, sparse: r.sparse, modelRevision: r.modelRevision };
	}

	async rerank(
		compute: Compute,
		query: string,
		candidates: { candidateId: string; text: string }[],
	): Promise<Map<string, number>> {
		const p = this.profile;
		const parts = [p.rerankProfileId, query];
		for (const c of candidates) parts.push(c.candidateId, c.text);
		const req: RerankRequest = {
			compute: { taskId: compute.taskId, generation: compute.generation, profileId: p.rerankProfileId },
			query,
			candidates,
			inputDigest: inputDigest(parts),
		};
		const r = (await this.post("rerankings", req)) as RerankResponse;
		if (r.inputDigest !== req.inputDigest) throw new InferenceMismatch("answer names another input digest");
		if (r.modelId !== "bge-reranker-v2-m3" || r.modelRevision !== p.rerankModelRevision)
			throw new InferenceMismatch(`model revision ${String(r.modelRevision)} is not the profile's`);
		const want = candidates.map((c) => c.candidateId);
		const got = Array.isArray(r.scores) ? r.scores.map((s) => s.candidateId) : [];
		if (got.length !== want.length || got.some((id, i) => id !== want[i]))
			throw new InferenceMismatch("answer scores other candidates");
		if (!r.scores.every((s) => Number.isFinite(s.score))) throw new InferenceMismatch("score is not finite");
		return new Map(r.scores.map((s) => [s.candidateId, s.score]));
	}
}
