// The S3 adapter against the MinIO of the development foundation's pinned
// image (Testcontainers): bounded reads, create-only copies and presigned
// URLs that work for exactly the signed object and method.
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ObjectMissing, ObjectTooLarge, S3Objects } from "../src/adapters/objects.js";

const minioImage =
	"minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";

let container: StartedTestContainer;
let store: S3Objects;

beforeAll(async () => {
	container = await new GenericContainer(minioImage)
		.withEnvironment({ MINIO_ROOT_USER: "knowledge-test", MINIO_ROOT_PASSWORD: "knowledge-test-secret" })
		.withCommand(["server", "/data"])
		.withExposedPorts(9000)
		.withWaitStrategy(Wait.forHttp("/minio/health/ready", 9000))
		.start();
	const endpoint = `http://${container.getHost()}:${container.getMappedPort(9000)}`;
	store = new S3Objects({
		endpoint,
		stageEndpoint: endpoint,
		bucket: "anvilkit-knowledge",
		region: "us-east-1",
		accessKeyId: "knowledge-test",
		secretAccessKey: "knowledge-test-secret",
	});
	const { S3Client, CreateBucketCommand } = await import("@aws-sdk/client-s3");
	const admin = new S3Client({
		endpoint,
		region: "us-east-1",
		forcePathStyle: true,
		credentials: { accessKeyId: "knowledge-test", secretAccessKey: "knowledge-test-secret" },
	});
	await admin.send(new CreateBucketCommand({ Bucket: "anvilkit-knowledge" }));
	admin.destroy();
}, 120_000);

afterAll(async () => {
	store?.close();
	await container?.stop();
});

describe("S3 objects on MinIO", () => {
	it("creates a content-addressed copy once and never replaces it", async () => {
		await store.putIfAbsent("sources/t/abc", Buffer.from("first"), "text/plain");
		await store.putIfAbsent("sources/t/abc", Buffer.from("second"), "text/plain");
		expect((await store.read("sources/t/abc", 100)).toString()).toBe("first");
		expect(await store.size("sources/t/abc")).toBe(5);
		expect(await store.size("sources/t/none")).toBeUndefined();
	});

	it("bounds reads and reports missing objects", async () => {
		await store.putIfAbsent("uploads/t/big", Buffer.alloc(4096, 1), "application/octet-stream");
		await expect(store.read("uploads/t/big", 4095)).rejects.toBeInstanceOf(ObjectTooLarge);
		await expect(store.read("uploads/t/missing", 10)).rejects.toBeInstanceOf(ObjectMissing);
	});

	it("presigns one GET and one PUT for exactly the signed key", async () => {
		const put = await store.presignPut("parse/k/result.json", 60);
		expect((await fetch(put, { method: "PUT", body: "{}" })).status).toBe(200);
		const get = await store.presignGet("parse/k/result.json", 60);
		expect(await (await fetch(get)).text()).toBe("{}");
		const other = get.replace("parse/k/result.json", "sources/t/abc");
		expect((await fetch(other)).status).toBe(403);
		expect((await fetch(get, { method: "DELETE" })).status).toBe(403);
	});
});
