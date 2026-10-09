// store-migrate: the vendor migration of the PostgresStore projection (P17,
// DD-07 §5). Runs the pinned Store's own migrations with the Store migration
// identity (never the domain migrator, never the runtime roles), then grants
// the runtime Store role DML on the vendor schema. The service itself runs
// the Store with ensureTables: false.
//
//	ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL=postgres://anvilkit_knowledge_store_migrator:…@…/anvilkit_knowledge?sslmode=verify-full&sslrootcert=… \
//	  node dist/storemigrate.js [--schema memory_store] [--role anvilkit_knowledge_store]
//
// The URL comes from the environment or, as the mounted secret file of P0.6,
// from ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL_FILE (exactly one of the two).
// It must verify the server (sslmode=verify-full) unless
// ANVILKIT_KNOWLEDGE_STORE_MIGRATION_DEVELOPMENT=true admits the plaintext
// development foundation (DEVELOPMENT_ONLY). The URL is never echoed.
import { migrateStore } from "./adapters/memorystore.js";
import { storeMigrationUrl } from "./config.js";

const fail = (message: string): never => {
	process.stderr.write(`store-migrate: ${message}\n`);
	process.exit(2);
};

const args = process.argv.slice(2);
const flag = (name: string, fallback: string) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 && args[i + 1] ? (args[i + 1] as string) : fallback;
};
let url = "";
try {
	url = storeMigrationUrl(process.env);
} catch (err) {
	fail(err instanceof Error ? err.message : String(err));
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
