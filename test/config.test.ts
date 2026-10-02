import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { inputsOf, loadFrom, parseApolloSnapshot } from "../src/config.js";

const base = { ANVILKIT_KNOWLEDGE_DATABASE_URL: "postgres://u:p@127.0.0.1:5432/anvilkit_knowledge" };

function write(content: string): string {
	const dir = mkdtempSync(path.join(tmpdir(), "knowledge-config-"));
	const p = path.join(dir, "config.yaml");
	writeFileSync(p, content);
	return p;
}

describe("configuration generations", () => {
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

	it("reads the secret file and a validated snapshot below the environment; rotation changes only the secret revision", () => {
		const dir = mkdtempSync(path.join(tmpdir(), "knowledge-config-"));
		const secret = path.join(dir, "database-url");
		writeFileSync(secret, "postgres://u:one@127.0.0.1:5432/anvilkit_knowledge\n");
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
		const env = { ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE: secret, ANVILKIT_KNOWLEDGE_APOLLO_SNAPSHOT_FILE: snapshot };
		const g1 = loadFrom(file, env, 1);
		expect(g1.config.tasks.maxAttempts).toBe(5);
		expect(g1.apolloRelease).toBe(good.releaseKey);
		writeFileSync(secret, "postgres://u:two@127.0.0.1:5432/anvilkit_knowledge");
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
			ANVILKIT_KNOWLEDGE_REMOVALS_ENDPOINT: "http://dr:9000",
			ANVILKIT_KNOWLEDGE_REMOVALS_CREDENTIALS_FILE: creds,
		};
		const g1 = loadFrom(file, env, 1);
		expect(g1.config.removals).toMatchObject({
			endpoint: "http://dr:9000",
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
