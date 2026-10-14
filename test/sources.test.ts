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
		() => ({ maxSourceBytes: 1 << 20, uploadTtlMs: 900_000, legacyUploads: true }),
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

	it("a source shared with a role is readable by a holder of that verified role and by nobody else (P0.3)", async () => {
		const shared = (
			await register(alice, "role-shared.md", "for reviewers\n", [
				{ principalType: "role", principalId: "reviewer" },
			] as never[])
		).source;
		const reviewer: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "carol", roles: ["reviewer"] };
		const author: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "dave", roles: ["author"] };
		expect((await sources.get(reviewer, shared.sourceId)).sourceId).toBe(shared.sourceId);
		expect(await codeOf(sources.get(author, shared.sourceId))).toBe("NOT_FOUND");
		expect(await codeOf(sources.get(bob, shared.sourceId))).toBe("NOT_FOUND");
		expect(await codeOf(sources.get({ ...reviewer, tenantId: "tenant_b" }, shared.sourceId))).toBe("NOT_FOUND");
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

		await sources.updateAccess(cmd(alice), alice, s.sourceId, 2, [{ principalType: "actor", principalId: "alice" }]);
		expect(await codeOf(sources.get(bob, s.sourceId))).toBe("NOT_FOUND");
		// source.authorization-revoked left the event catalog (P2.8): the live
		// read gate enforces the shrink and the ACL history records it.
		expect(await eventsOf("source.authorization-revoked")).toHaveLength(0);
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
		expect(await eventsOf("source.authorization-revoked")).toHaveLength(0);
	});

	it("a scope naming another project reads, changes and deletes nothing (the listing's rule)", async () => {
		const s = (await register(alice, "scoped.md", "scoped\n")).source;
		const otherProject: Scope = { ...alice, projectId: "proj_b" };
		expect(await codeOf(sources.get(otherProject, s.sourceId))).toBe("NOT_FOUND");
		expect(
			await codeOf(
				sources.updateAccess(cmd(otherProject), otherProject, s.sourceId, 1, [
					{ principalType: "actor", principalId: "alice" },
				]),
			),
		).toBe("NOT_FOUND");
		expect(await codeOf(sources.delete(cmd(otherProject), otherProject, s.sourceId, 1))).toBe("NOT_FOUND");
		const tenantWide: Scope = { ...alice, projectId: "" };
		expect((await sources.get(tenantWide, s.sourceId)).sourceId).toBe(s.sourceId);
		expect((await sources.list(tenantWide, "", 200)).sources.map((x) => x.sourceId)).toContain(s.sourceId);
		expect((await sources.list(otherProject, "", 200)).sources.map((x) => x.sourceId)).not.toContain(s.sourceId);
	});
});

describe("media types", () => {
	it("admits HTML exactly when the parser will (its marker rule)", async () => {
		const html = async (name: string, text: string) => {
			const u = upload(alice, name, text);
			return codeOf(
				sources.register(
					cmd(alice),
					alice,
					{ kind: "document", locator: u.locator, contentDigest: u.digest, mediaType: "text/html", sizeBytes: u.size },
					[],
				),
			);
		};
		expect(await html("a.html", "<!DOCTYPE html><html><body>x</body></html>")).toBe("OK");
		expect(await html("b.html", "<html lang=en><body>x</body></html>")).toBe("OK");
		expect(await html("c.html", "<!DOCTYPE  html><body>x</body>")).toBe("SOURCE_UNVERIFIED");
		expect(await html("d.html", "<!doctype\nhtml><body>x</body>")).toBe("SOURCE_UNVERIFIED");
	});
});

describe("upload capability (P2.6)", () => {
	const strict = () =>
		new Sources(
			store,
			tasks,
			() => objects,
			() => plan,
			() => ({ maxSourceBytes: 1 << 20, uploadTtlMs: 900_000, legacyUploads: false }),
			clock,
			silentLogger,
		);
	const declare = (text: string) => {
		const bytes = Buffer.from(text);
		return { bytes, contentDigest: digestOf(bytes), sizeBytes: String(bytes.length), mediaType: "text/markdown" };
	};
	const put = (key: string, bytes: Buffer) => objects.objects.set(key, bytes);
	const registerUpload = (s: Sources, scope: Scope, uploadId: string, d: ReturnType<typeof declare>, c = cmd(scope)) =>
		s.register(
			c,
			scope,
			{
				kind: "document",
				locator: `upload:${uploadId}`,
				contentDigest: d.contentDigest,
				mediaType: d.mediaType,
				sizeBytes: d.sizeBytes,
			},
			[],
		);

	it("issues a presigned PUT bound to the declaration; the same actor registers it once", async () => {
		const s = strict();
		const d = declare("# Brand guide\n\nTeal hero, single column.\n");
		const c = cmd(alice);
		const cap = await s.beginUpload(c, alice, d);
		expect(cap.uploadId).toMatch(/^upl-[0-9a-f]{32}$/);
		expect(cap.method).toBe("PUT");
		expect(cap.headers).toEqual({
			"content-length": d.sizeBytes,
			"content-type": "text/markdown",
			"x-amz-checksum-sha256": Buffer.from(d.contentDigest.slice(7), "hex").toString("base64"),
		});
		expect(cap.expiresAt.getTime()).toBe(clock.now().getTime() + 900_000);
		expect(objects.uploads.get(`uploads/tenant_a/${cap.uploadId}`)?.ttlSeconds).toBe(900);
		const again = await s.beginUpload(c, alice, d);
		expect(again).toMatchObject({ uploadId: cap.uploadId, existing: true });
		expect(await codeOf(s.beginUpload({ ...c, requestDigest: `sha256:${"9".repeat(64)}` }, alice, d))).toBe(
			"COMMAND_CONFLICT",
		);
		put(`uploads/tenant_a/${cap.uploadId}`, d.bytes);
		expect(await codeOf(registerUpload(s, bob, cap.uploadId, d))).toBe("SOURCE_UNVERIFIED");
		expect(await codeOf(registerUpload(s, mallory, cap.uploadId, d))).toBe("SOURCE_UNVERIFIED");
		const ok = await registerUpload(s, alice, cap.uploadId, d);
		expect(ok.existing).toBe(false);
		expect(await codeOf(registerUpload(s, alice, cap.uploadId, d))).toBe("SOURCE_UNVERIFIED");
		const row = await store.pool.query<{ source_id: string }>(
			"SELECT source_id FROM source_uploads WHERE upload_id = $1",
			[cap.uploadId],
		);
		expect(row.rows[0]?.source_id).toBe(ok.source.sourceId);
		expect(await codeOf(s.beginUpload(c, alice, d))).toBe("SOURCE_UNVERIFIED");
	});

	it("refuses a registration after the TTL, with another size or digest, or without a capability", async () => {
		const s = strict();
		const d = declare("# Tone\n\nShort sentences.\n");
		const late = await s.beginUpload(cmd(alice), alice, d);
		put(`uploads/tenant_a/${late.uploadId}`, d.bytes);
		clock.advance(900_001);
		expect(await codeOf(registerUpload(s, alice, late.uploadId, d))).toBe("SOURCE_UNVERIFIED");
		const cap = await s.beginUpload(cmd(alice), alice, d);
		put(`uploads/tenant_a/${cap.uploadId}`, d.bytes);
		expect(await codeOf(registerUpload(s, alice, cap.uploadId, { ...d, sizeBytes: String(d.bytes.length + 1) }))).toBe(
			"SOURCE_UNVERIFIED",
		);
		expect(
			await codeOf(registerUpload(s, alice, cap.uploadId, { ...d, contentDigest: `sha256:${"0".repeat(64)}` })),
		).toBe("SOURCE_UNVERIFIED");
		const swapped = Buffer.from("# Tone\n\nLong sentences!\n");
		put(`uploads/tenant_a/${cap.uploadId}`, swapped);
		expect(await codeOf(registerUpload(s, alice, cap.uploadId, d))).toBe("SOURCE_UNVERIFIED");
		const legacy = upload(alice, "fixture-direct.md", "# Direct\n");
		expect(
			await codeOf(
				s.register(
					cmd(alice),
					alice,
					{
						kind: "document",
						locator: legacy.locator,
						contentDigest: legacy.digest,
						mediaType: "text/markdown",
						sizeBytes: legacy.size,
					},
					[],
				),
			),
		).toBe("SOURCE_UNVERIFIED");
		expect(await codeOf(s.beginUpload(cmd(alice), alice, { ...d, mediaType: "application/x-unknown" }))).toBe(
			"UNSUPPORTED_SOURCE",
		);
		expect(await codeOf(s.beginUpload(cmd(alice), alice, { ...d, sizeBytes: String((1 << 20) + 1) }))).toBe(
			"SOURCE_TOO_LARGE",
		);
	});
});

describe("manage permission (P2.7)", () => {
	it("a reader neither replaces the access list nor deletes; the owner does; a manage entry grants both", async () => {
		const reg = await register(alice, "managed.md", "# Managed\n\nOwned by alice.\n", [
			{ principalType: "tenant", principalId: "tenant_a" },
		] as never[]);
		const id = reg.source.sourceId;
		expect(reg.source.ownerId).toBe("alice");
		expect(await codeOf(sources.get(bob, id))).toBe("OK");
		expect(
			await codeOf(sources.updateAccess(cmd(bob), bob, id, 1, [{ principalType: "actor", principalId: "bob" }])),
		).toBe("FORBIDDEN");
		expect(await codeOf(sources.delete(cmd(bob), bob, id, 1))).toBe("FORBIDDEN");
		// The owner grants bob manage (and keeps the tenant's read).
		const granted = await sources.updateAccess(cmd(alice), alice, id, 1, [
			{ principalType: "tenant", principalId: "tenant_a" },
			{ principalType: "actor", principalId: "bob", permission: "manage" },
		]);
		expect(granted.source.access).toContainEqual({ principalType: "actor", principalId: "bob", permission: "manage" });
		const byBob = await sources.updateAccess(cmd(bob), bob, id, 2, [
			{ principalType: "tenant", principalId: "tenant_a" },
			{ principalType: "role", principalId: "curators", permission: "manage" },
		]);
		expect(byBob.source.aclRevision).toBe(3);
		// A manage entry for a role is held only through verified roles.
		const carol: Scope = { tenantId: "tenant_a", projectId: "proj_a", actorId: "carol" };
		expect(await codeOf(sources.delete(cmd(carol), carol, id, 1))).toBe("FORBIDDEN");
		const curator: Scope = { ...carol, roles: ["curators"] };
		const deleted = await sources.delete(cmd(curator), curator, id, 1);
		expect(deleted.source.deleted).toBe(true);
	});
});
