import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace";
import { describe, expect, it } from "vitest";
import { Telemetry } from "../src/telemetry.js";

describe("telemetry", () => {
	it("records one span per RPC with the method and status code only, continuing the caller's trace", async () => {
		const exporter = new InMemorySpanExporter();
		const t = new Telemetry({ otlpEndpoint: "", sampleRatio: 1 }, "test", new SimpleSpanProcessor({ exporter }));
		const end = t.rpc("anvilkit.knowledge.v1.SearchRequest", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
		end(7);
		end(0); // a second end is ignored
		await new Promise((resolve) => setImmediate(resolve));
		const spans = exporter.getFinishedSpans();
		expect(spans).toHaveLength(1);
		expect(spans[0]?.name).toBe("anvilkit.knowledge.v1.Search");
		expect(spans[0]?.attributes).toEqual({
			"rpc.system": "grpc",
			"rpc.method": "anvilkit.knowledge.v1.Search",
			"rpc.grpc.status_code": 7,
		});
		expect(spans[0]?.spanContext().traceId).toBe("0af7651916cd43dd8448eb211c80319c");
		await t.shutdown();
	});
});
