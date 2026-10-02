// P23: the removal inventory's S3 adapter against the MinIO of the
// development foundation's pinned image (Testcontainers): records are
// created once and never replaced, a retried create of the same bytes is
// confirmed by reading it back, other bytes under the key are a conflict,
// reads are bounded, listings are ordered and paginated without gaps, and
// an unreachable store is an uncertain write or an incomplete listing.
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	RemovalConflict,
	RemovalListIncomplete,
	RemovalTooLarge,
	RemovalUncertain,
	S3RemovalInventory,
} from "../src/adapters/removals.js";

const minioImage =
	"minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";

let container: StartedTestContainer;
let inv: S3RemovalInventory;

const cfg = (endpoint: string) => ({
	endpoint,
	bucket: "anvilkit-memory-removals",
	region: "us-east-1",
	accessKeyId: "knowledge-test",
	secretAccessKey: "knowledge-test-secret",
	timeoutMs: 5_000,
});

beforeAll(async () => {
	container = await new GenericContainer(minioImage)
		.withEnvironment({ MINIO_ROOT_USER: "knowledge-test", MINIO_ROOT_PASSWORD: "knowledge-test-secret" })
		.withCommand(["server", "/data"])
		.withExposedPorts(9000)
		.withWaitStrategy(Wait.forHttp("/minio/health/ready", 9000))
		.start();
	const endpoint = `http://${container.getHost()}:${container.getMappedPort(9000)}`;
	inv = new S3RemovalInventory(cfg(endpoint));
	const { S3Client, CreateBucketCommand } = await import("@aws-sdk/client-s3");
	const admin = new S3Client({
		endpoint,
		region: "us-east-1",
		forcePathStyle: true,
		credentials: { accessKeyId: "knowledge-test", secretAccessKey: "knowledge-test-secret" },
	});
	await admin.send(new CreateBucketCommand({ Bucket: "anvilkit-memory-removals" }));
	admin.destroy();
}, 120_000);

afterAll(async () => {
	inv?.close();
	await container?.stop();
});

describe("S3 removal inventory on MinIO", () => {
	it("creates a record once; the same bytes again are confirmed, other bytes are a conflict", async () => {
		const key = "removals/20261002T120000.000Z/a.json";
		await inv.create(key, Buffer.from('{"n":1}'));
		await inv.create(key, Buffer.from('{"n":1}'));
		await expect(inv.create(key, Buffer.from('{"n":2}'))).rejects.toBeInstanceOf(RemovalConflict);
		await expect(inv.create(key, Buffer.from('{"n":1} longer'))).rejects.toBeInstanceOf(RemovalConflict);
		expect((await inv.read(key, 100))?.toString()).toBe('{"n":1}');
	});

	it("bounds reads and reports an absent record as undefined", async () => {
		await inv.create("removals/20261002T120001.000Z/big.json", Buffer.alloc(64, 1));
		await expect(inv.read("removals/20261002T120001.000Z/big.json", 63)).rejects.toBeInstanceOf(RemovalTooLarge);
		expect(await inv.read("removals/20261002T120001.000Z/none.json", 10)).toBeUndefined();
	});

	it("lists keys after a floor in key order, page by page, without a gap", async () => {
		const keys = [
			"removals/20261002T130000.000Z/1.json",
			"removals/20261002T130000.001Z/2.json",
			"removals/20261002T130001.000Z/3.json",
			"removals/20261002T130002.000Z/4.json",
			"removals/20261002T130003.000Z/5.json",
		];
		for (const k of [...keys].reverse()) await inv.create(k, Buffer.from(k));
		const seen: string[] = [];
		let next: string | undefined;
		let pages = 0;
		do {
			const page = await inv.list("removals/", "removals/20261002T130000.000Z/", next, 2);
			seen.push(...page.keys);
			next = page.next;
			pages++;
		} while (next);
		expect(seen).toEqual(keys);
		expect(pages).toBe(3);
		// A floor after the first two keys starts at the third.
		const later = await inv.list("removals/", "removals/20261002T130001.000Z/", undefined, 100);
		expect(later.keys).toEqual(keys.slice(2));
		expect(later.next).toBeUndefined();
	});

	it("an unreachable store is an uncertain create and an incomplete listing, never an empty one", async () => {
		const down = new S3RemovalInventory({ ...cfg("http://127.0.0.1:9"), timeoutMs: 1_000 });
		await expect(down.create("removals/x.json", Buffer.from("x"))).rejects.toBeInstanceOf(RemovalUncertain);
		await expect(down.list("removals/", "", undefined, 10)).rejects.toBeInstanceOf(RemovalListIncomplete);
		down.close();
	});
});
