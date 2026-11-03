// The wire mapping shared by AdvanceIndex and AdvanceProjection: each answer
// state carries exactly its own fields.
import { IndexState } from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { describe, expect, it } from "vitest";
import { indexAnswerOf } from "../src/transport/protomap.js";

describe("indexAnswerOf", () => {
	it("maps running, materialized and failed answers to their IndexState and fields", () => {
		expect(indexAnswerOf({ state: "running", retryAfterMs: 250 })).toEqual({
			state: IndexState.INDEX_STATE_RUNNING,
			resultRef: "",
			resultDigest: "",
			failureCode: undefined,
			retryAfterMs: 250,
		});
		expect(indexAnswerOf({ state: "materialized", resultRef: "index:1:src:1", resultDigest: "sha256:ab" })).toEqual({
			state: IndexState.INDEX_STATE_MATERIALIZED,
			resultRef: "index:1:src:1",
			resultDigest: "sha256:ab",
			failureCode: undefined,
			retryAfterMs: 0,
		});
		expect(indexAnswerOf({ state: "failed", failureCode: "CHUNK_UNVERIFIED" })).toEqual({
			state: IndexState.INDEX_STATE_FAILED,
			resultRef: "",
			resultDigest: "",
			failureCode: "CHUNK_UNVERIFIED",
			retryAfterMs: 0,
		});
	});
});
