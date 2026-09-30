// memoryctl: the operator entry of the memory projections' recovery (P17,
// DD-07 §5). `rebuild --target N` requests the projection of every fact the
// target (0 = the PostgresStore, g = the vector collection of generation g)
// held or should hold, from the authoritative facts and under a new epoch;
// the Background Worker then applies them. Revoked, expired and deleted
// facts are rebuilt as tombstones: a rebuild never revives a removal. The
// same configuration and database identity as the service; output is one
// JSON line.
import { parseArgs } from "node:util";
import { Registry } from "prom-client";
import { newPool, Store } from "./adapters/postgres.js";
import { MemoryProjector } from "./application/projection.js";
import { noDispatchQuery, systemClock, Tasks } from "./application/tasks.js";
import { load } from "./config.js";
import { jsonLogger } from "./log.js";
import { Metrics } from "./metrics.js";

async function main(argv: string[]): Promise<void> {
	const { values, positionals } = parseArgs({
		args: argv,
		options: { target: { type: "string" } },
		allowPositionals: true,
	});
	if (positionals[0] !== "rebuild" || !/^(0|[1-9][0-9]{0,15})$/.test(values.target ?? ""))
		throw new Error("usage: memoryctl rebuild --target <0 (store) | generation>");
	const gen = load();
	const pool = newPool(gen.config.database.url, 2);
	const log = jsonLogger((line) => process.stderr.write(`${line}\n`));
	const metrics = new Metrics(new Registry());
	const t = gen.config.tasks;
	const store = new Store(pool);
	const tasks = new Tasks(
		store,
		noDispatchQuery,
		{
			maxInputBytes: t.maxInputBytes,
			maxLeaseMs: t.maxLeaseMs,
			retryDelayMs: t.retryDelayMs,
			maxAttempts: t.maxAttempts,
		},
		systemClock,
		log,
		metrics,
	);
	const projector = new MemoryProjector(
		store,
		tasks,
		() => undefined,
		() => undefined,
		() => undefined,
		() => undefined,
		() => gen.config.memory,
		systemClock,
		log,
		metrics,
	);
	try {
		const target = Number(values.target);
		const requested = await projector.rebuild(target);
		process.stdout.write(`${JSON.stringify({ target, requested })}\n`);
	} finally {
		await pool.end();
	}
}

main(process.argv.slice(2)).catch((err) => {
	process.stderr.write(`memoryctl: ${err instanceof Error ? err.message : String(err)}\n`);
	process.exit(1);
});
