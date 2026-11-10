// The clients the process holds beyond a generation: Control's disclosure
// and dispatch clients and the shared identity watcher close on a failed
// start and at the final shutdown, never at a generation's retirement. No
// database: the generation is a stand-in whose pool answers every query.
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import type pg from "pg";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ControlDisclosure, ControlDispatchQuery } from "../src/adapters/control.js";
import { type Generation, loadFrom } from "../src/config.js";
import { buildRuntime, type Runtime, retire } from "../src/generations.js";
import { start } from "../src/main.js";
import { IdentityWatcher } from "../src/transport/identity.js";
import { silentLogger } from "./logger.js";
import { files, issue, mount, newCA, spiffe, tempDir } from "./pki.js";

vi.mock("../src/generations.js", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../src/generations.js")>();
	return { ...actual, buildRuntime: vi.fn(actual.buildRuntime) };
});

const ca = newCA("startup-ca");
const identityDir = tempDir();
mount(
	identityDir,
	issue(ca, "anvilkit-agent-knowledge", [spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-knowledge")]),
	ca.pem,
);

function listening(): Promise<Server> {
	return new Promise((resolve) => {
		const s = createServer();
		s.listen(0, "127.0.0.1", () => resolve(s));
	});
}

function address(s: Server): string {
	return `127.0.0.1:${(s.address() as { port: number }).port}`;
}

async function freeAddress(): Promise<string> {
	const s = await listening();
	const a = address(s);
	await new Promise<void>((r) => s.close(() => r()));
	return a;
}

// A development generation with the mTLS listener identity and a Control
// placement (the disclosure and dispatch clients exist); the database is
// a refused port, probed within the shortest connect bound.
function generation(environ: NodeJS.ProcessEnv): Generation {
	const dir = mkdtempSync(path.join(tmpdir(), "knowledge-startup-"));
	const file = path.join(dir, "config.yaml");
	writeFileSync(file, "development:\n  enabled: true\ndatabase:\n  connect_timeout: 1s\n");
	return loadFrom(file, environ, 1);
}

function environ(grpc: string, health: string): NodeJS.ProcessEnv {
	return {
		ANVILKIT_KNOWLEDGE_DATABASE_URL: "postgres://u:p@127.0.0.1:1/anvilkit_knowledge?sslmode=disable",
		ANVILKIT_KNOWLEDGE_LISTEN: grpc,
		ANVILKIT_KNOWLEDGE_HEALTH_LISTEN: health,
		ANVILKIT_KNOWLEDGE_CONTROL_ADDRESS: "127.0.0.1:1",
		ANVILKIT_KNOWLEDGE_IDENTITY_CERT_FILE: files(identityDir).certFile,
		ANVILKIT_KNOWLEDGE_IDENTITY_KEY_FILE: files(identityDir).keyFile,
		ANVILKIT_KNOWLEDGE_IDENTITY_CA_FILE: files(identityDir).caFile,
		ANVILKIT_KNOWLEDGE_IDENTITY_TRUST_DOMAIN: "anvilkit.local",
	};
}

/** The stand-in generation: every query answers with a current schema version. */
function standIn(gen: Generation, identity: IdentityWatcher): Runtime {
	const pool = {
		query: async () => ({ rows: [{ v: "999" }], rowCount: 1 }),
		connect: async () => ({ query: async () => ({ rows: [] }), release: () => {} }),
		end: async () => {},
		on: () => {},
	} as unknown as pg.Pool;
	return { gen, pool, identity };
}

function watcher(): IdentityWatcher {
	const w = new IdentityWatcher(files(identityDir), 50);
	w.start();
	return w;
}

afterEach(() => {
	vi.restoreAllMocks();
});

describe("process-held clients", () => {
	it("a failed start closes the Control clients and stops the shared identity watcher", async () => {
		const occupied = await listening();
		try {
			const env = environ(await freeAddress(), address(occupied));
			const gen = generation(env);
			const w = watcher();
			const stopped = vi.spyOn(w, "stop");
			const disclosure = vi.spyOn(ControlDisclosure.prototype, "close");
			const dispatch = vi.spyOn(ControlDispatchQuery.prototype, "close");
			vi.mocked(buildRuntime).mockResolvedValueOnce(standIn(gen, w));
			await expect(start(gen, silentLogger, env)).rejects.toThrow();
			expect(disclosure).toHaveBeenCalledTimes(1);
			expect(dispatch).toHaveBeenCalledTimes(1);
			expect(stopped).toHaveBeenCalledTimes(1);
		} finally {
			await new Promise<void>((r) => occupied.close(() => r()));
		}
	});

	it("the final shutdown closes the Control clients and stops the shared identity watcher", async () => {
		const env = environ(await freeAddress(), await freeAddress());
		const gen = generation(env);
		const w = watcher();
		const stopped = vi.spyOn(w, "stop");
		const disclosure = vi.spyOn(ControlDisclosure.prototype, "close");
		vi.mocked(buildRuntime).mockResolvedValueOnce(standIn(gen, w));
		const running = await start(gen, silentLogger, env);
		expect(disclosure).not.toHaveBeenCalled();
		expect(stopped).not.toHaveBeenCalled();
		await running.stop();
		expect(disclosure).toHaveBeenCalledTimes(1);
		expect(stopped).toHaveBeenCalledTimes(1);
	});

	it("a generation's retirement leaves the shared identity watcher running", async () => {
		const env = environ(await freeAddress(), await freeAddress());
		const w = watcher();
		const stopped = vi.spyOn(w, "stop");
		expect(await retire(standIn(generation(env), w), 1000)).toBe(false);
		expect(stopped).not.toHaveBeenCalled();
		w.stop();
	});

	it("a rejected first generation stops the watcher it created; a reused one stays", async () => {
		const env = environ(await freeAddress(), await freeAddress());
		const gen = generation(env);
		const started = vi.spyOn(IdentityWatcher.prototype, "start");
		const stopped = vi.spyOn(IdentityWatcher.prototype, "stop");
		await expect(buildRuntime(gen)).rejects.toThrow(/database probe/);
		expect(started).toHaveBeenCalledTimes(1);
		expect(stopped).toHaveBeenCalledTimes(1);

		const shared = watcher();
		stopped.mockClear();
		await expect(buildRuntime(gen, standIn(gen, shared))).rejects.toThrow(/database probe/);
		expect(stopped).not.toHaveBeenCalled();
		shared.stop();
	});
});
