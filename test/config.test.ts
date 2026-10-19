import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { inputsOf, loadFrom, parseApolloSnapshot, storeMigrationUrl } from "../src/config.js";

// P0.1: the identity files are placements the loader does not read.
const identity = {
	ANVILKIT_KNOWLEDGE_IDENTITY_CERT_FILE: "/etc/anvilkit/identity/tls.crt",
	ANVILKIT_KNOWLEDGE_IDENTITY_KEY_FILE: "/etc/anvilkit/identity/tls.key",
	ANVILKIT_KNOWLEDGE_IDENTITY_CA_FILE: "/etc/anvilkit/identity/ca.crt",
	ANVILKIT_KNOWLEDGE_IDENTITY_TRUST_DOMAIN: "anvilkit.local",
};
// Outside development every PostgreSQL URL verifies the server (P0.6).
const base = {
	ANVILKIT_KNOWLEDGE_DATABASE_URL: "postgres://u:p@db.anvilkit-data.svc:5432/anvilkit_knowledge?sslmode=verify-full",
	...identity,
};

function write(content: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), "knowledge-config-"));
	const p = path.join(dir, "config.yaml");
	writeFileSync(p, content);
	return p;
}

describe("configuration generations", () => {
	it("defaults to an mTLS listener and refuses plaintext without the development guard (P0.1)", () => {
		const g = loadFrom(write("{}\n"), base, 1);
		expect(g.config.grpc.identity.mode).toBe("mtls");
		expect(g.config.grpc.identity.trustDomain).toBe("anvilkit.local");
		expect(g.config.control.identity).toEqual({ mode: "mtls", serverName: "anvilkit-agent-control" });
		expect(g.config.development.enabled).toBe(false);
		const { ANVILKIT_KNOWLEDGE_IDENTITY_TRUST_DOMAIN: _td, ...noDomain } = base;
		const { ANVILKIT_KNOWLEDGE_IDENTITY_CERT_FILE: _c, ...noCert } = base;
		const cases: [string, NodeJS.ProcessEnv, RegExp][] = [
			["{}\n", noCert, /grpc.identity.cert_file, key_file and ca_file are required/],
			["{}\n", noDomain, /grpc.identity.trust_domain is required outside development/],
			["grpc:\n  identity:\n    trust_domain: Not_A_Domain\n", noDomain, /trust_domain must be a lowercase DNS name/],
			[
				"grpc:\n  identity:\n    mode: development\n",
				base,
				/grpc.identity.mode development .* requires development.enabled/,
			],
			["grpc:\n  identity:\n    mode: plaintext\n", base, /grpc.identity.mode must be mtls or development/],
			[
				"control:\n  identity:\n    mode: development\n",
				base,
				/control.identity.mode development .* requires development.enabled/,
			],
			[
				"inference:\n  tls:\n    mode: development\n",
				{ ...base, ANVILKIT_KNOWLEDGE_INFERENCE_URL: "http://inference:8000" },
				/inference.tls.mode development .* requires development.enabled/,
			],
			["{}\n", { ...base, ANVILKIT_KNOWLEDGE_INFERENCE_URL: "http://inference:8000" }, /inference.url is plaintext/],
			[
				"development:\n  enabled: true\ninference:\n  tls:\n    mode: development\n",
				{ ...base, ANVILKIT_KNOWLEDGE_INFERENCE_URL: "https://inference:8000" },
				/inference.url is https/,
			],
			[
				"inference:\n  tls:\n    mode: mtls\n",
				{ ...base, ANVILKIT_KNOWLEDGE_INFERENCE_URL: "https://inference:8000" },
				/inference.tls.ca_file, cert_file and key_file are required/,
			],
			["grpc:\n  identity:\n    max_connection_age: 10s\n", base, /max_connection_age/],
			["{}\n", { ...base, ANVILKIT_KNOWLEDGE_DEVELOPMENT_ENABLED: "true" }, /not allowed overrides/],
		];
		for (const [file, env, re] of cases) expect(() => loadFrom(write(file), env, 1), file).toThrow(re);
		// The guard admits the development modes without downgrading the others.
		const dev = loadFrom(
			write("development:\n  enabled: true\ngrpc:\n  identity:\n    mode: development\n"),
			noDomain,
			1,
		);
		expect(dev.config.grpc.identity.mode).toBe("development");
		expect(dev.config.grpc.identity.trustDomain).toBe("anvilkit.local");
		expect(dev.config.control.identity.mode).toBe("mtls");
		const plainInference = loadFrom(
			write("development:\n  enabled: true\ninference:\n  tls:\n    mode: development\n"),
			{ ...base, ANVILKIT_KNOWLEDGE_INFERENCE_URL: "http://127.0.0.1:8000" },
			1,
		);
		expect(plainInference.config.inference.tls.mode).toBe("development");
		expect(plainInference.config.grpc.identity.mode).toBe("mtls");
	});

	it("loads the reviewed file and never carries the secret in a digest", () => {
		const g = loadFrom(path.resolve("config.yaml"), base, 1);
		expect(g.config.tasks.maxAttempts).toBe(3);
		expect(g.digest).toMatch(/^sha256:/);
		expect(inputsOf(g)).not.toContain("postgres://");
	});

	it("rejects unknown keys, secrets and placements in the file, unknown environment and cross-field conflicts", () => {
		const cases: [string, NodeJS.ProcessEnv, RegExp][] = [
			["grpc:\n  bogus: 1\n", base, /unknown key/],
			["database:\n  url: postgres://x\n", base, /secret or a placement/],
			["control:\n  address: x:1\n", base, /secret or a placement/],
			["{}\n", { ...base, ANVILKIT_KNOWLEDGE_SURPRISE: "1" }, /not allowed overrides/],
			["{}\n", {}, /database.url is required/],
			["tasks:\n  max_lease: 1s\n  sweep_interval: 2s\n", base, /shorter than tasks.max_lease/],
			["tasks:\n  max_input_bytes: 70000\n", base, /max_input_bytes/],
			["apollo:\n  mode: snapshot\n", base, /apollo.snapshot_file is required/],
			// P16: the Qdrant placement and key are environment-only; the retrieval bounds must nest.
			["qdrant:\n  api_key: k\n", base, /secret or a placement/],
			["qdrant:\n  url: http://q:6333\n", base, /secret or a placement/],
			["{}\n", { ...base, ANVILKIT_KNOWLEDGE_QDRANT_URL: "http://q:6333" }, /qdrant.api_key is required/],
			["qdrant:\n  replication_factor: 1\n  write_consistency_factor: 2\n", base, /write_consistency_factor/],
			["retrieval:\n  rerank_limit: 50\n  fused_limit: 40\n", base, /rerank_limit must not exceed/],
			["retrieval:\n  min_rerank_score: high\n", base, /min_rerank_score must be a decimal/],
			// P23: the removal inventory's placement and credentials are environment-only and required together.
			["removals:\n  endpoint: http://dr:9000\n", base, /secret or a placement/],
			["removals:\n  secret_access_key: s\n", base, /secret or a placement/],
			[
				"{}\n",
				{ ...base, ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "http://dr:9000" },
				/removals credentials are required/,
			],
			["{}\n", { ...base, ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "dr:9000" }, /removals.endpoint must be an HTTP/],
			["removals:\n  window_margin: 2d\n", base, /removals.window_margin/],
			[
				"removals:\n  bucket: anvilkit-knowledge\n",
				{
					...base,
					ANVILKIT_KNOWLEDGE_OBJECTS_ENDPOINT: "http://minio:9000",
					ANVILKIT_KNOWLEDGE_OBJECTS_ACCESS_KEY_ID: "a",
					ANVILKIT_KNOWLEDGE_OBJECTS_SECRET_ACCESS_KEY: "s",
					ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "http://minio:9000",
					ANVILKIT_KNOWLEDGE_REMOVALS_ACCESS_KEY_ID: "a",
					ANVILKIT_KNOWLEDGE_REMOVALS_SECRET_ACCESS_KEY: "s",
				},
				/must not be the objects bucket/,
			],
		];
		for (const [file, env, want] of cases) expect(() => loadFrom(write(file), env, 1)).toThrow(want);
	});

	it("places the retained Inference of an embedding-model change from the environment with its complete earlier space (P2.7)", () => {
		const env = {
			...base,
			ANVILKIT_KNOWLEDGE_INFERENCE_URL: "https://inference-next:8000",
			ANVILKIT_KNOWLEDGE_INFERENCE_RETAINED_URL: "https://inference:8000",
		};
		const file = (retained: string) =>
			`inference:\n  embedding_profile: bge-m3-v2\n  embedding_revision: next\n  tls:\n    ca_file: /etc/ca.crt\n${retained}`;
		const earlier =
			"  retained:\n    embedding_profile: bge-m3-v1\n    embedding_revision: 5617a9f61b028005a4858fdac845db406aefb181\n    dimensions: 1024\n    sparse_profile: bge-m3-lexical-v1\n";
		const g = loadFrom(write(file(earlier)), env, 1);
		expect(g.config.inference.retained).toEqual({
			url: "https://inference:8000",
			serverName: "",
			embeddingProfile: "bge-m3-v1",
			embeddingRevision: "5617a9f61b028005a4858fdac845db406aefb181",
			dimensions: 1024,
			sparseProfile: "bge-m3-lexical-v1",
		});
		// Unplaced, the keys are inert.
		const { ANVILKIT_KNOWLEDGE_INFERENCE_RETAINED_URL: _r, ...unplaced } = env;
		expect(loadFrom(write(file(earlier)), unplaced, 1).config.inference.retained.url).toBe("");
		const { ANVILKIT_KNOWLEDGE_INFERENCE_URL: _a, ...noActive } = env;
		const cases: [string, NodeJS.ProcessEnv, RegExp][] = [
			[file("  retained:\n    url: https://inference:8000\n"), unplaced, /secret or a placement/],
			[file(earlier), noActive, /inference.retained.url needs the active placement/],
			[file(earlier.replace(/ {4}embedding_revision: .*\n/, "")), env, /embedding_revision is required/],
			[file(earlier.replace(/ {4}dimensions: .*\n/, "")), env, /inference.retained.dimensions/],
			[
				file(earlier),
				{ ...env, ANVILKIT_KNOWLEDGE_INFERENCE_RETAINED_URL: "http://inference:8000" },
				/transport of inference.tls.mode/,
			],
			// The active profile left at the defaults, which are the retained space.
			[`inference:\n  tls:\n    ca_file: /etc/ca.crt\n${earlier}`, env, /names the active embedding space/],
		];
		for (const [text, e, want] of cases) expect(() => loadFrom(write(text), e, 1), text).toThrow(want);
	});

	it("reads the secret file and a validated snapshot below the environment; rotation changes only the secret revision", () => {
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-config-"));
		const secret = path.join(dir, "database-url");
		writeFileSync(secret, "postgres://u:one@db.anvilkit-data.svc:5432/anvilkit_knowledge?sslmode=verify-full\n");
		const now = new Date();
		const snapshot = path.join(dir, "apollo.json");
		const good = {
			schemaVersion: 1,
			appId: "anvilkit-agent-knowledge",
			cluster: "default",
			namespace: "application",
			releaseKey: "20260917120000-0123456789ab",
			fetchedAt: now.toISOString(),
			expiresAt: new Date(now.getTime() + 3_600_000).toISOString(),
			configurations: { "tasks.max_attempts": "5" },
		};
		writeFileSync(snapshot, JSON.stringify(good));
		const file = write("apollo:\n  mode: snapshot\n");
		const env = {
			ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE: secret,
			ANVILKIT_KNOWLEDGE_APOLLO_SNAPSHOT_FILE: snapshot,
			...identity,
		};
		const g1 = loadFrom(file, env, 1);
		expect(g1.config.tasks.maxAttempts).toBe(5);
		expect(g1.apolloRelease).toBe(good.releaseKey);
		writeFileSync(secret, "postgres://u:two@db.anvilkit-data.svc:5432/anvilkit_knowledge?sslmode=verify-full");
		const g2 = loadFrom(file, env, 2);
		expect(g2.digest).toBe(g1.digest);
		expect(g2.secretRevision).not.toBe(g1.secretRevision);
		const bad: Record<string, unknown> = {
			expired: { ...good, expiresAt: new Date(now.getTime() - 60_000).toISOString() },
			"wrong app": { ...good, appId: "anvilkit-agent-mcp" },
			"secret key": { ...good, configurations: { "database.url": "postgres://x" } },
			"unknown field": { ...good, extra: true },
			"release key": { ...good, releaseKey: "release-1" },
			"unknown config key": { ...good, configurations: { "tasks.bogus": "1" } },
		};
		for (const [name, content] of Object.entries(bad)) {
			writeFileSync(snapshot, JSON.stringify(content));
			expect(() => loadFrom(file, env, 3), name).toThrow();
		}
		expect(() => parseApolloSnapshot("not json", "anvilkit-agent-knowledge", now)).toThrow(/apollo snapshot/);
	});

	it("places the removal inventory from the environment and a mounted credentials file; rotation is a new secret revision", () => {
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-config-"));
		const creds = path.join(dir, "removals.env");
		writeFileSync(creds, "AWS_ACCESS_KEY_ID=knowledge-removals\nAWS_SECRET_ACCESS_KEY=one\n");
		const file = write("removals:\n  window_margin: 10m\n");
		const env = {
			...base,
			ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "https://dr:9000",
			ANVILKIT_KNOWLEDGE_REMOVALS_CREDENTIALS_FILE: creds,
		};
		const g1 = loadFrom(file, env, 1);
		expect(g1.config.removals).toMatchObject({
			endpoint: "https://dr:9000",
			bucket: "anvilkit-memory-removals",
			accessKeyId: "knowledge-removals",
			windowMarginMs: 600_000,
			reconcileIntervalMs: 30_000,
		});
		expect(inputsOf(g1)).not.toContain("one");
		writeFileSync(creds, "AWS_ACCESS_KEY_ID=knowledge-removals\nAWS_SECRET_ACCESS_KEY=two\n");
		const g2 = loadFrom(file, env, 2);
		expect(g2.digest).toBe(g1.digest);
		expect(g2.secretRevision).not.toBe(g1.secretRevision);
		// Unplaced by default: memory serves without the preservation (the service warns).
		expect(loadFrom(path.resolve("config.yaml"), base, 1).config.removals.endpoint).toBe("");
	});

	it("reads the Qdrant key and the Store URL from mounted secret files, never both sources; rotation is a new secret revision (P0.6)", () => {
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-config-"));
		const keyFile = path.join(dir, "qdrant-api-key");
		const storeFile = path.join(dir, "store-url");
		writeFileSync(keyFile, "qdrant-key-one\n");
		writeFileSync(storeFile, "postgres://store:one@db:5432/anvilkit_knowledge?sslmode=verify-full\n");
		const file = write("{}\n");
		const env = {
			...base,
			ANVILKIT_KNOWLEDGE_QDRANT_URL: "https://qdrant:6333",
			ANVILKIT_KNOWLEDGE_QDRANT_API_KEY_FILE: keyFile,
			ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL_FILE: storeFile,
		};
		const g1 = loadFrom(file, env, 1);
		expect(g1.config.qdrant.apiKey).toBe("qdrant-key-one");
		expect(g1.config.store.url).toBe("postgres://store:one@db:5432/anvilkit_knowledge?sslmode=verify-full");
		expect(inputsOf(g1)).not.toContain("qdrant-key-one");
		writeFileSync(keyFile, "qdrant-key-two\n");
		const g2 = loadFrom(file, env, 2);
		expect(g2.digest).toBe(g1.digest);
		expect(g2.secretRevision).not.toBe(g1.secretRevision);
		const cases: [NodeJS.ProcessEnv, RegExp][] = [
			[
				{ ...env, ANVILKIT_KNOWLEDGE_QDRANT_API_KEY: "inline" },
				/qdrant.api_key and qdrant.api_key_file are mutually exclusive/,
			],
			[
				{ ...env, ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL: "postgres://x@db/k?sslmode=verify-full" },
				/store.url and store.url_file are mutually exclusive/,
			],
			[{ ...env, ANVILKIT_KNOWLEDGE_QDRANT_API_KEY_FILE: path.join(dir, "missing") }, /qdrant.api_key_file: ENOENT/],
			[{ ...env, ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL_FILE: write("") }, /store.url_file: the file is empty/],
		];
		for (const [e, want] of cases) expect(() => loadFrom(file, e, 3)).toThrow(want);
		// The file keys are placements: never accepted from the reviewed file.
		expect(() => loadFrom(write(`qdrant:\n  api_key_file: ${keyFile}\n`), base, 1)).toThrow(/secret or a placement/);
		expect(() => loadFrom(write(`store:\n  url_file: ${storeFile}\n`), base, 1)).toThrow(/secret or a placement/);
	});

	it("refuses plaintext data-plane endpoints and unverified PostgreSQL URLs outside development, never echoing the URL (P0.6)", () => {
		const secretUrl = "postgres://u:hunter2@db:5432/anvilkit_knowledge";
		const cases: [NodeJS.ProcessEnv, RegExp][] = [
			[
				{ ...base, ANVILKIT_KNOWLEDGE_DATABASE_URL: secretUrl },
				/database.url: sslmode must be verify-full outside development \(got none\)/,
			],
			[
				{ ...base, ANVILKIT_KNOWLEDGE_DATABASE_URL: `${secretUrl}?sslmode=require` },
				/database.url: sslmode must be verify-full outside development \(got "require"\)/,
			],
			[
				// node-postgres takes the last of repeated parameters.
				{ ...base, ANVILKIT_KNOWLEDGE_DATABASE_URL: `${secretUrl}?sslmode=verify-full&sslmode=disable` },
				/database.url: sslmode must be verify-full .*got "verify-full,disable"/,
			],
			[
				{ ...base, ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL: `${secretUrl}?sslmode=require` },
				/store.url: sslmode must be verify-full outside development \(got "require"\)/,
			],
			[
				// node-postgres would check an IP host's certificate against "localhost".
				{
					...base,
					ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL: "postgres://u:hunter2@10.0.0.7:5432/k?sslmode=verify-full",
				},
				/store.url: the host must be a DNS name outside development/,
			],
			[
				{ ...base, ANVILKIT_KNOWLEDGE_DATABASE_URL: `${secretUrl}?sslmode=verify-full&host=127.0.0.1` },
				/database.url: the host must be a DNS name outside development/,
			],
			[
				{
					...base,
					ANVILKIT_KNOWLEDGE_OBJECTS_ENDPOINT: "http://minio:9000",
					ANVILKIT_KNOWLEDGE_OBJECTS_ACCESS_KEY_ID: "a",
					ANVILKIT_KNOWLEDGE_OBJECTS_SECRET_ACCESS_KEY: "s",
				},
				/objects.endpoint must be https:\/\/ outside development/,
			],
			[
				{
					...base,
					ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "http://dr:9000",
					ANVILKIT_KNOWLEDGE_REMOVALS_ACCESS_KEY_ID: "a",
					ANVILKIT_KNOWLEDGE_REMOVALS_SECRET_ACCESS_KEY: "s",
				},
				/removals.endpoint must be https:\/\/ outside development/,
			],
			[
				{ ...base, ANVILKIT_KNOWLEDGE_QDRANT_URL: "http://qdrant:6333", ANVILKIT_KNOWLEDGE_QDRANT_API_KEY: "k" },
				/qdrant.url must be https:\/\/ outside development/,
			],
		];
		for (const [env, want] of cases) {
			let message = "";
			try {
				loadFrom(write("{}\n"), env, 1);
			} catch (err) {
				message = (err as Error).message;
			}
			expect(message).toMatch(want);
			expect(message).not.toContain("hunter2");
			expect(message).not.toContain("minio:9000");
		}
		// The verified forms load outside development.
		const secure = loadFrom(
			write("{}\n"),
			{
				...base,
				ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL: `${secretUrl}?sslmode=verify-full&sslrootcert=/etc/anvilkit/pg/ca.crt`,
				ANVILKIT_KNOWLEDGE_OBJECTS_ENDPOINT: "https://minio:9000",
				ANVILKIT_KNOWLEDGE_OBJECTS_ACCESS_KEY_ID: "a",
				ANVILKIT_KNOWLEDGE_OBJECTS_SECRET_ACCESS_KEY: "s",
				ANVILKIT_KNOWLEDGE_QDRANT_URL: "https://qdrant:6333",
				ANVILKIT_KNOWLEDGE_QDRANT_API_KEY: "k",
			},
			1,
		);
		expect(secure.config.development.enabled).toBe(false);
		expect(secure.config.qdrant.url).toBe("https://qdrant:6333");
		// The guard admits the plaintext foundation (DEVELOPMENT_ONLY).
		const dev = loadFrom(
			write("development:\n  enabled: true\n"),
			{
				...base,
				ANVILKIT_KNOWLEDGE_DATABASE_URL: `${secretUrl}?sslmode=disable`,
				ANVILKIT_KNOWLEDGE_STORE_DATABASE_URL: secretUrl,
				ANVILKIT_KNOWLEDGE_OBJECTS_ENDPOINT: "http://minio:9000",
				ANVILKIT_KNOWLEDGE_OBJECTS_ACCESS_KEY_ID: "a",
				ANVILKIT_KNOWLEDGE_OBJECTS_SECRET_ACCESS_KEY: "s",
				ANVILKIT_KNOWLEDGE_QDRANT_URL: "http://qdrant:6333",
				ANVILKIT_KNOWLEDGE_QDRANT_API_KEY: "k",
			},
			1,
		);
		expect(dev.config.objects.endpoint).toBe("http://minio:9000");
	});

	it("resolves the Store migration URL from exactly one source and refuses an unverified URL unless admitted (P0.6)", () => {
		const verified = "postgres://m:secret@db:5432/anvilkit_knowledge?sslmode=verify-full";
		const plain = "postgres://m:secret@db:5432/anvilkit_knowledge?sslmode=disable";
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-store-migration-"));
		const file = path.join(dir, "url");
		writeFileSync(file, `${verified}\n`);
		expect(storeMigrationUrl({ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL: verified })).toBe(verified);
		expect(storeMigrationUrl({ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL_FILE: file })).toBe(verified);
		expect(
			storeMigrationUrl({
				ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL: plain,
				ANVILKIT_KNOWLEDGE_STORE_MIGRATION_DEVELOPMENT: "true",
			}),
		).toBe(plain);
		const cases: [NodeJS.ProcessEnv, RegExp][] = [
			[{}, /exactly one of ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL and ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL_FILE/],
			[
				{ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL: verified, ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL_FILE: file },
				/exactly one of/,
			],
			[
				{ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL: plain },
				/sslmode must be verify-full outside development \(got "disable"\)/,
			],
			[
				{ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL: plain, ANVILKIT_KNOWLEDGE_STORE_MIGRATION_DEVELOPMENT: "false" },
				/sslmode must be verify-full/,
			],
			[
				{ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL: plain, ANVILKIT_KNOWLEDGE_STORE_MIGRATION_DEVELOPMENT: "yes" },
				/must be true or false/,
			],
			[{ ANVILKIT_KNOWLEDGE_STORE_MIGRATION_URL_FILE: path.join(dir, "missing") }, /_URL_FILE: ENOENT/],
		];
		for (const [env, want] of cases) {
			expect(() => storeMigrationUrl(env)).toThrow(want);
			try {
				storeMigrationUrl(env);
			} catch (err) {
				expect((err as Error).message).not.toContain("secret");
			}
		}
	});
});

describe("shared snapshot fixtures", () => {
	it("agrees with packages/profile-schemas/fixtures.json when the parent checkout is present", async () => {
		const { existsSync, readFileSync } = await import("node:fs");
		let d = process.cwd();
		let file = "";
		for (;;) {
			const c = path.join(d, "packages", "profile-schemas", "fixtures.json");
			if (existsSync(c)) {
				file = c;
				break;
			}
			const parent = path.dirname(d);
			if (parent === d) break;
			d = parent;
		}
		if (!file) return; // UNEXECUTED outside the parent checkout
		const doc = JSON.parse(readFileSync(file, "utf8")) as {
			now: string;
			cases: { name: string; schema: string; appId?: string; valid: boolean; instance: unknown }[];
		};
		const now = new Date(doc.now);
		for (const c of doc.cases.filter((x) => x.schema === "apollo-snapshot")) {
			let ok = true;
			try {
				parseApolloSnapshot(JSON.stringify(c.instance), c.appId ?? "", now);
			} catch {
				ok = false;
			}
			expect(ok, c.name).toBe(c.valid);
		}
	});
});
