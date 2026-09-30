// store-migrate: the vendor migration of the PostgresStore projection (P17,
// DD-07 §5). Runs the pinned Store's own migrations with the Store migration
// identity (never the domain migrator, never the runtime roles), then grants
// the runtime Store role DML on the vendor schema. The service itself runs
// the Store with ensureTables: false.
//
//	ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL=postgres://anvilkit_knowledge_store_migrator:…@…/anvilkit_knowledge \
//	  node dist/storemigrate.js [--schema memory_store] [--role anvilkit_knowledge_store]
import { migrateStore } from "./adapters/memorystore.js";

const args = process.argv.slice(2);
const flag = (name: string, fallback: string) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 && args[i + 1] ? (args[i + 1] as string) : fallback;
};
const url = process.env.ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL ?? "";
if (!url) {
	process.stderr.write("store-migrate: ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL is required\n");
	process.exit(2);
}
const schema = flag("schema", "memory_store");
migrateStore(url, schema, flag("role", "anvilkit_knowledge_store"))
	.then((v) => {
		process.stdout.write(`store schema=${schema} version=${v}\n`);
		process.exit(0);
	})
	.catch((err) => {
		process.stderr.write(`store-migrate: ${err instanceof Error ? err.message : String(err)}\n`);
		process.exit(1);
	});
