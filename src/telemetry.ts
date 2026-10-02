// Knowledge's spans (security.md "data classification, logging and
// deletion"): one server span per RPC named after the RPC, with the gRPC
// status code only. A source body, retrieved text, a memory fact, a tenant,
// a principal or any request field never becomes a span attribute. The
// callers' trace context (API, Background Worker, Control-side callers)
// continues through the traceparent metadata. Spans go over OTLP/HTTP to the
// collector when an endpoint is placed and nowhere otherwise.
import { defaultTextMapGetter, ROOT_CONTEXT, SpanKind, SpanStatusCode, type Tracer, trace } from "@opentelemetry/api";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
	BatchSpanProcessor,
	ParentBasedSampler,
	type SpanProcessor,
	TraceIdRatioBasedSampler,
	TracerProvider,
} from "@opentelemetry/sdk-trace";

/** Observes one RPC: returns the function that records its gRPC status code. */
export interface RpcObserver {
	rpc(requestType: string, traceparent: string | undefined): (code: number) => void;
}

export class Telemetry implements RpcObserver {
	private readonly tracer: Tracer;
	private readonly provider: TracerProvider | undefined;
	private readonly propagator = new W3CTraceContextPropagator();

	constructor(cfg: { otlpEndpoint: string; sampleRatio: number }, service: string, processor?: SpanProcessor) {
		const spans =
			processor ??
			(cfg.otlpEndpoint
				? new BatchSpanProcessor({
						exporter: new OTLPTraceExporter({ url: `${cfg.otlpEndpoint.replace(/\/$/, "")}/v1/traces` }),
					})
				: undefined);
		if (spans) {
			this.provider = new TracerProvider({
				resource: resourceFromAttributes({ "service.name": service }),
				sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(cfg.sampleRatio) }),
				spanProcessors: [spans],
			});
			this.tracer = this.provider.getTracer(service);
		} else {
			this.tracer = trace.getTracer(service);
		}
	}

	rpc(requestType: string, traceparent: string | undefined): (code: number) => void {
		// "anvilkit.knowledge.v1.ClaimTaskRequest" -> service package and method.
		const method = requestType.replace(/Request$/, "");
		const parent = this.propagator.extract(ROOT_CONTEXT, { traceparent }, defaultTextMapGetter);
		const span = this.tracer.startSpan(
			method,
			{ kind: SpanKind.SERVER, attributes: { "rpc.system": "grpc", "rpc.method": method } },
			parent,
		);
		let ended = false;
		return (code: number) => {
			if (ended) return;
			ended = true;
			span.setAttribute("rpc.grpc.status_code", code);
			if (code !== 0) span.setStatus({ code: SpanStatusCode.ERROR });
			span.end();
		};
	}

	async shutdown(): Promise<void> {
		await this.provider?.shutdown();
	}
}
