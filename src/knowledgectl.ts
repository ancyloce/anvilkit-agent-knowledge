// knowledgectl: the operator entry of the source lifecycle (P2.7).
//   purge-status <sourceId>          whether a source is live, deleted with
//                                    its erasure pending, or purged (its
//                                    upload, unshared content-addressed copy,
//                                    parse results, chunk rows and points
//                                    removed)
//   rebuild documents [--generation N]
//                                    writes the document points of a
//                                    generation again (default: the accepted
//                                    one) as new task generations the
//                                    Background Worker completes
// The same configuration and database identity as the service; output is
// one JSON line.
import { parseArgs } from "node:util";
import { Registry } from "prom-client";
import * as idx from "./adapters/indexdb.js";
import { newPool, Store } from "./adapters/postgres.js";
import { Cleanup } from "./application/cleanup.js";
import { Indexer } from "./application/indexer.js";
import { noDispatchQuery, systemClock, Tasks } from "./application/tasks.js";
import { load } from "./config.js";
import { jsonLogger } from "./log.js";
import { Metrics } from "./metrics.js";

const usage = "usage: knowledgectl purge-status <sourceId> | rebuild documents [--generation N]";

async function main(argv: string[]): Promise<void> {
	const { values, positionals } = parseArgs({
		args: argv,
		options: { generation: { type: "string" } },
		allowPositionals: true,
	});
	const [command, arg] = positionals;
	const purgeStatus = command === "purge-status" && !!arg && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(arg);
	const rebuild =
		command === "rebuild" &&
		arg === "documents" &&
		(values.generation === undefined || /^[1-9][0-9]{0,15}$/.test(values.generation));
	if (!purgeStatus && !rebuild) throw new Error(usage);
	const gen = load();
	const pool = newPool(gen.config.database.url, 2);
	const log = jsonLogger((line) => process.stderr.write(`${line}\n`));
	const metrics = new Metrics(new Registry());
	const store = new Store(pool);
	try {
		if (purgeStatus) {
			const cleanup = new Cleanup(
				store,
				() => undefined,
				() => undefined,
				() => ({ ...gen.config.cleanup, consumerGroup: gen.config.outbox.consumerGroup }),
				systemClock,
				log,
				metrics,
			);
			const status = await cleanup.status(arg as string);
			if (!status) throw new Error(`no source ${arg}`);
			process.stdout.write(`${JSON.stringify(status)}\n`);
			return;
		}
		const t = gen.config.tasks;
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
		const indexer = new Indexer(
			store,
			tasks,
			() => undefined,
			() => undefined,
			() => undefined,
			() => undefined,
			() => ({
				batchSize: gen.config.index.batchSize,
				maxResultBytes: gen.config.index.maxResultBytes,
				pollMs: gen.config.index.pollMs,
			}),
			systemClock,
			log,
			metrics,
		);
		const generation = values.generation
			? Number(values.generation)
			: (await idx.listGenerations(pool)).find((g) => g.state === "accepted")?.generation;
		if (!generation) throw new Error("no accepted index generation (name one with --generation)");
		const requested = await indexer.rebuildDocuments(generation);
		process.stdout.write(`${JSON.stringify({ generation, requested })}\n`);
	} finally {
		await pool.end();
	}
}

main(process.argv.slice(2)).catch((err) => {
	process.stderr.write(`knowledgectl: ${err instanceof Error ? err.message : String(err)}\n`);
	process.exit(1);
});
