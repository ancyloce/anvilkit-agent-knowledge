import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryObjects } from "../src/adapters/objects.js";
import type { Store } from "../src/adapters/postgres.js";
import { type IngestPlan, Sources } from "../src/application/sources.js";
import type { Tasks } from "../src/application/tasks.js";
import { type Command, parseIngestInput, type Scope, SourceError } from "../src/domain/source.js";
import { digestOf } from "../src/domain/task.js";
import { silentLogger } from "../src/log.js";
import { FakeClock, FakeDispatch, type Instance, newTasks, startInstance } from "./harness.js";

let inst: Instance;
let tasks: Tasks;
let store: Store;
let sources: Sources;
const objects = new MemoryObjects();
const clock = new FakeClock(new Date("2026-09-18T12:00:00Z"));

const plan: IngestPlan = {
	profileId: "parser-docling-dev-v1",
	profileRevision: 1,
	chunkerId: "docling-hierarchical",
	chunkerRevision: 1,
	mediaTypes: ["application/pdf", "text/markdown", "text/plain", "text/html"],
	maxInputBytes: 1 << 20,
};

const alice: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "alice" };
const bob: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "bob" };
const mallory: Scope = { tenantId: "tenant_b", projectId: "proj_a", actorId: "alice" };

let seq = 0;
function cmd(scope: Scope, digest = `sha256:${"1".repeat(64)}`): Command {
	seq++;
	return { tenantId: scope.tenantId, commandId: `cmd_${seq}`, actorId: scope.actorId, requestDigest: digest };
}

function upload(scope: Scope, name: string, text: string | Buffer): { locator: string; digest: string; size: string } {
	const bytes = Buffer.isBuffer(text) ? text : Buffer.from(text);
	objects.objects.set(`uploads/${scope.tenantId}/${name}`, bytes);
	return { locator: `upload:${name}`, digest: digestOf(bytes), size: String(bytes.length) };
}

async function register(scope: Scope, name: string, text: string, access = [] as never[], c = cmd(scope)) {
	const u = upload(scope, name, text);
	return sources.register(
		c,
		scope,
		{ kind: "document", locator: u.locator, contentDigest: u.digest, mediaType: "text/markdown", sizeBytes: u.size },
		access,
	);
}

async function codeOf(p: Promise<unknown>): Promise<string> {
	try {
		await p;
	} catch (err) {
		if (err instanceof SourceError) return err.code;
		throw err;
	}
	return "OK";
}

async function eventsOf(type: string): Promise<Record<string, unknown>[]> {
	const r = await store.pool.query<{ payload: string }>(
		`SELECT payload::text AS payload FROM outbox ORDER BY transaction_id, "offset"`,
	);
	return r.rows
		.map((row) => {
			const w = JSON.parse(row.payload) as { payload: string };
			return JSON.parse(Buffer.from(w.payload, "base64").toString("utf8")) as Record<string, unknown>;
		})
		.filter((e) => e.eventType === type);
}

beforeAll(async () => {
	inst = await startInstance();
	({ tasks, store } = newTasks(inst, clock, new FakeDispatch()));
	sources = new Sources(
		store,
		tasks,
		() => objects,
		() => plan,
		() => ({ maxSourceBytes: 1 << 20 }),
		clock,
		silentLogger,
	);
});

afterAll(async () => {
	await store.pool.end();
	await inst.stop();
});

describe("registration", () => {
	it("commits the verified revision, trusted ACL, ingest request and knowledge-ingest request together", async () => {
		const out = await register(alice, "brand.md", "# Brand\n\nTeal and slate.\n");
		expect(out.existing).toBe(false);
		const s = out.source;
		expect(s.sourceId).toMatch(/^src-[0-9a-f]{32}$/);
		expect(s.access).toEqual([{ principalType: "actor", principalId: "alice" }]);
		expect(s.ingest).toBe("pending");
		expect(s.objectKey).toBe(`sources/tenant_a/${s.contentDigest.slice(7)}`);
		expect(objects.objects.get(s.objectKey)?.toString()).toContain("Teal");

		const task = await tasks.get(`ingest-${s.sourceId}-r1`);
		expect(task.kind).toBe("knowledge-ingest");
		expect(task.state).toBe("pending");
		expect(task.resultProfile).toBe("knowledge-ingest-v1");
		expect(task.authorizationRef).toBe(`source:${s.sourceId}`);
		const frozen = await store.pool.query<{ input: string }>(
			"SELECT input::text AS input FROM background_requests WHERE task_id = $1",
			[task.taskId],
		);
		const input = parseIngestInput(frozen.rows[0]?.input ?? "");
		expect(input.input).toEqual({
			digest: s.contentDigest,
			sizeBytes: String(s.sizeBytes),
			mediaType: "text/markdown",
		});
		expect(input.parserProfile).toBe("parser-docling-dev-v1");
		const requested = await eventsOf("background.requested");
		expect(requested.some((e) => e.aggregateId === task.taskId)).toBe(true);
	});

	it("replays the same command and refuses the same command with another request", async () => {
		const c = cmd(alice);
		const first = await register(alice, "replay.md", "replay\n", [], c);
		const again = await register(alice, "replay.md", "replay\n", [], c);
		expect(again.existing).toBe(true);
		expect(again.source.sourceId).toBe(first.source.sourceId);
		expect(
			await codeOf(register(alice, "replay.md", "replay\n", [], { ...c, requestDigest: `sha256:${"2".repeat(64)}` })),
		).toBe("COMMAND_CONFLICT");
	});

	it("rejects bytes that differ from the description, oversized and unsupported sources", async () => {
		const u = upload(alice, "lie.md", "actual bytes\n");
		const req = {
			kind: "document" as const,
			locator: u.locator,
			contentDigest: u.digest,
			mediaType: "text/markdown",
			sizeBytes: u.size,
		};
		expect(
			await codeOf(sources.register(cmd(alice), alice, { ...req, contentDigest: `sha256:${"f".repeat(64)}` }, [])),
		).toBe("SOURCE_UNVERIFIED");
		expect(await codeOf(sources.register(cmd(alice), alice, { ...req, sizeBytes: "3" }, []))).toBe("SOURCE_UNVERIFIED");
		expect(await codeOf(sources.register(cmd(alice), alice, { ...req, mediaType: "application/pdf" }, []))).toBe(
			"SOURCE_UNVERIFIED",
		);
		expect(await codeOf(sources.register(cmd(alice), alice, { ...req, sizeBytes: String((1 << 20) + 1) }, []))).toBe(
			"SOURCE_TOO_LARGE",
		);
		const big = upload(alice, "big.md", Buffer.alloc((1 << 20) + 10, 0x61));
		expect(
			await codeOf(
				sources.register(
					cmd(alice),
					alice,
					{ ...req, locator: big.locator, contentDigest: big.digest, sizeBytes: "100" },
					[],
				),
			),
		).toBe("SOURCE_TOO_LARGE");
		expect(
			await codeOf(sources.register(cmd(alice), alice, { ...req, kind: "url", locator: "https://example.com" }, [])),
		).toBe("UNSUPPORTED_SOURCE");
		expect(await codeOf(sources.register(cmd(alice), alice, { ...req, mediaType: "application/zip" }, []))).toBe(
			"UNSUPPORTED_SOURCE",
		);
		expect(await codeOf(sources.register(cmd(alice), alice, { ...req, locator: "upload:../escape" }, []))).toBe(
			"INVALID_ARGUMENT",
		);
		expect(await codeOf(sources.register(cmd(alice), alice, { ...req, locator: "upload:missing.md" }, []))).toBe(
			"SOURCE_UNVERIFIED",
		);
	});

	it("never accepts a caller-assigned ACL outside the command's scope or an assumed identity", async () => {
		const other = [{ principalType: "tenant" as const, principalId: "tenant_b" }];
		expect(await codeOf(register(alice, "acl1.md", "x\n", other as never[]))).toBe("FORBIDDEN");
		const project = [{ principalType: "project" as const, principalId: "proj_z" }];
		expect(await codeOf(register(alice, "acl2.md", "x\n", project as never[]))).toBe("FORBIDDEN");
		expect(await codeOf(register(alice, "acl3.md", "x\n", [], { ...cmd(alice), actorId: "bob" }))).toBe("FORBIDDEN");
		expect(await codeOf(register(mallory, "acl4.md", "x\n", [], { ...cmd(mallory), tenantId: "tenant_a" }))).toBe(
			"FORBIDDEN",
		);
	});
});

describe("readability, access and deletion", () => {
	it("discloses a source only to principals of its current ACL and pages over readable rows only", async () => {
		const a = (await register(alice, "private.md", "private\n")).source;
		expect(await codeOf(sources.get(bob, a.sourceId))).toBe("NOT_FOUND");
		expect(await codeOf(sources.get(mallory, a.sourceId))).toBe("NOT_FOUND");
		expect((await sources.list(bob, "", 200)).sources.map((s) => s.sourceId)).not.toContain(a.sourceId);

		const mine = await sources.list(alice, "", 2);
		expect(mine.sources).toHaveLength(2);
		const rest = await sources.list(alice, mine.nextCursor, 200);
		const all = [...mine.sources, ...rest.sources].map((s) => s.sourceId);
		expect(new Set(all).size).toBe(all.length);
		expect(all).toContain(a.sourceId);
		expect(await codeOf(sources.list(alice, Buffer.from("x' OR 1=1").toString("base64url"), 10))).toBe(
			"INVALID_ARGUMENT",
		);
	});

	it("replaces the ACL under its expected revision and publishes a revocation when access shrinks", async () => {
		const s = (await register(alice, "shared.md", "shared\n")).source;
		const grant = [
			{ principalType: "actor" as const, principalId: "alice" },
			{ principalType: "actor" as const, principalId: "bob" },
		];
		const granted = await sources.updateAccess(cmd(alice), alice, s.sourceId, 1, grant);
		expect(granted.source.aclRevision).toBe(2);
		expect((await sources.get(bob, s.sourceId)).sourceId).toBe(s.sourceId);
		expect(await codeOf(sources.updateAccess(cmd(alice), alice, s.sourceId, 1, grant))).toBe("REVISION_MISMATCH");
		expect(await codeOf(sources.updateAccess(cmd(bob), bob, "src-00000000000000000000000000000000", 1, grant))).toBe(
			"NOT_FOUND",
		);

		const before = (await eventsOf("source.authorization-revoked")).length;
		await sources.updateAccess(cmd(alice), alice, s.sourceId, 2, [{ principalType: "actor", principalId: "alice" }]);
		expect(await codeOf(sources.get(bob, s.sourceId))).toBe("NOT_FOUND");
		const revoked = await eventsOf("source.authorization-revoked");
		expect(revoked).toHaveLength(before + 1);
		expect(revoked.at(-1)?.payload).toEqual({
			kind: "source.authorization-revoked",
			sourceId: s.sourceId,
			aclRevision: "3",
		});
		const history = await store.pool.query("SELECT acl_revision FROM source_acl_revisions WHERE source_id = $1", [
			s.sourceId,
		]);
		expect(history.rowCount).toBe(3);
	});

	it("deletion blocks readability in its transaction, cancels the open parse and is idempotent", async () => {
		const s = (await register(alice, "gone.md", "gone\n")).source;
		const c = cmd(alice);
		expect(await codeOf(sources.delete(c, alice, s.sourceId, 2))).toBe("REVISION_MISMATCH");
		const out = await sources.delete(c, alice, s.sourceId, 1);
		expect(out.source.deleted).toBe(true);
		expect(out.source.access).toEqual([]);
		expect(await codeOf(sources.get(alice, s.sourceId))).toBe("NOT_FOUND");
		expect((await tasks.get(`ingest-${s.sourceId}-r1`)).state).toBe("canceled");
		expect((await sources.delete(c, alice, s.sourceId, 1)).existing).toBe(true);
		const revoked = await eventsOf("source.authorization-revoked");
		expect(revoked.some((e) => e.aggregateId === s.sourceId)).toBe(true);
	});
});
