// DEVELOPMENT_ONLY owner entry point of the local-check fixture kind:
// `request`, `cancel` and `get` run the same application code and
// transactions the service uses (there is no public create RPC; the P15
// source lifecycle creates its own requests). Output is one JSON line.
import { randomUUID } from "node:crypto";
import { parseArgs } from "node:util";
import { Registry } from "prom-client";
import { newPool, Store } from "./adapters/postgres.js";
import { noDispatchQuery, systemClock, Tasks } from "./application/tasks.js";
import { load } from "./config.js";
import { expectedResult, localCheckProfile } from "./domain/task.js";
import { jsonLogger } from "./log.js";
import { Metrics } from "./metrics.js";

async function main(argv: string[]): Promise<void> {
	const command = argv[0];
	const { values } = parseArgs({
		args: argv.slice(1),
		options: {
			"task-id": { type: "string" },
			tenant: { type: "string", default: "tenant_a" },
			source: { type: "string" },
			bytes: { type: "string", default: "hello" },
			"hold-ms": { type: "string", default: "0" },
			fail: { type: "boolean", default: false },
			"wrong-digest": { type: "boolean", default: false },
			effects: { type: "string", default: "reconstructible" },
			"dispatch-id": { type: "string", default: "" },
		},
	});
	const gen = load();
	const pool = newPool(gen.config.database.url, 2);
	const log = jsonLogger((line) => process.stderr.write(`${line}\n`));
	const t = gen.config.tasks;
	const tasks = new Tasks(
		new Store(pool),
		noDispatchQuery,
		{
			maxInputBytes: t.maxInputBytes,
			maxLeaseMs: t.maxLeaseMs,
			retryDelayMs: t.retryDelayMs,
			maxAttempts: t.maxAttempts,
		},
		systemClock,
		log,
		new Metrics(new Registry()),
	);
	const emit = (o: unknown) => process.stdout.write(`${JSON.stringify(o)}\n`);
	try {
		switch (command) {
			case "request": {
				if (!values.source) throw new Error("--source is required (the current authorization the request is bound to)");
				const taskId = values["task-id"] || `task_${randomUUID()}`;
				const input = JSON.stringify({
					schemaVersion: 1,
					computation: localCheckProfile,
					bytes: Buffer.from(values.bytes ?? "").toString("base64"),
					...(Number(values["hold-ms"]) > 0 ? { holdMs: Number(values["hold-ms"]) } : {}),
					...(values.fail ? { fail: true } : {}),
					...(values["wrong-digest"] ? { wrongDigest: true } : {}),
				});
				const { task, existing } = await tasks.request({
					taskId,
					tenantId: values.tenant ?? "tenant_a",
					kind: "local-check",
					profile: localCheckProfile,
					input,
					effects: values.effects as "reconstructible" | "external",
					dispatchId: values["dispatch-id"] ?? "",
					authorizationRef: `source:${values.source}`,
					correlationId: `local-check-${randomUUID()}`,
				});
				const expected = expectedResult(task);
				emit({
					taskId: task.taskId,
					generation: task.generation,
					inputDigest: task.inputDigest,
					existing,
					expectedResultRef: expected.ref,
					expectedResultDigest: expected.digest,
				});
				break;
			}
			case "cancel": {
				const { task, changed } = await tasks.cancel(values["task-id"] ?? "");
				emit({ taskId: task.taskId, generation: task.generation, state: task.state, changed });
				break;
			}
			case "get": {
				const task = await tasks.get(values["task-id"] ?? "");
				emit({
					taskId: task.taskId,
					generation: task.generation,
					state: task.state,
					attemptCount: task.attemptCount,
					workerId: task.workerId,
					resultDigest: task.resultDigest,
					failureCode: task.failureCode,
					revision: task.revision,
				});
				break;
			}
			default:
				throw new Error(
					"usage: localcheck request|cancel|get [--task-id] [--tenant] [--source] [--bytes] [--hold-ms] [--fail] [--wrong-digest] [--effects] [--dispatch-id]",
				);
		}
	} finally {
		await pool.end();
	}
}

main(process.argv.slice(2)).catch((err) => {
	process.stderr.write(`error: ${err instanceof Error ? err.message : String(err)}\n`);
	process.exit(1);
});
