// Knowledge's object store (the anvilkit-knowledge bucket, its own user and
// bucket-limited policy): callers' uploads under uploads/<tenant>/, the
// verified, content-addressed source copies under sources/<tenant>/ and the
// parser Jobs' outputs under parse/. Reads are bounded before and while the
// body streams; source copies are immutable (created only when absent). The
// parser Pod never receives these credentials: it gets presigned URLs for
// exactly one GET and one PUT, signed for the endpoint the Pod reaches. No
// key or URL is logged.
import {
	GetObjectCommand,
	type GetObjectCommandOutput,
	HeadObjectCommand,
	NoSuchKey,
	NotFound,
	PutObjectCommand,
	S3Client,
	S3ServiceException,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export interface ObjectsConfig {
	endpoint: string;
	/** The endpoint parser Pods reach; presigned URLs are signed for it. */
	stageEndpoint: string;
	bucket: string;
	region: string;
	accessKeyId: string;
	secretAccessKey: string;
}

export class ObjectTooLarge extends Error {}
export class ObjectMissing extends Error {}

/** The port the use cases depend on (tests use an in-memory one). */
export interface ObjectStore {
	size(key: string): Promise<number | undefined>;
	read(key: string, maxBytes: number): Promise<Buffer>;
	putIfAbsent(key: string, bytes: Buffer, contentType: string): Promise<void>;
	presignGet(key: string, ttlSeconds: number): Promise<string>;
	presignPut(key: string, ttlSeconds: number): Promise<string>;
	close(): void;
}

export class S3Objects implements ObjectStore {
	private readonly client: S3Client;
	private readonly signer: S3Client;

	constructor(private readonly cfg: ObjectsConfig) {
		const credentials = { accessKeyId: cfg.accessKeyId, secretAccessKey: cfg.secretAccessKey };
		this.client = new S3Client({
			endpoint: cfg.endpoint,
			region: cfg.region,
			forcePathStyle: true,
			credentials,
			maxAttempts: 1,
		});
		this.signer = new S3Client({
			endpoint: cfg.stageEndpoint || cfg.endpoint,
			region: cfg.region,
			forcePathStyle: true,
			credentials,
		});
	}

	async size(key: string): Promise<number | undefined> {
		try {
			const h = await this.client.send(new HeadObjectCommand({ Bucket: this.cfg.bucket, Key: key }));
			return h.ContentLength ?? 0;
		} catch (err) {
			if (err instanceof NotFound || err instanceof NoSuchKey || status(err) === 404) return undefined;
			throw err;
		}
	}

	async read(key: string, maxBytes: number): Promise<Buffer> {
		let out: GetObjectCommandOutput;
		try {
			out = await this.client.send(new GetObjectCommand({ Bucket: this.cfg.bucket, Key: key }));
		} catch (err) {
			if (err instanceof NoSuchKey || status(err) === 404) throw new ObjectMissing("object missing");
			throw err;
		}
		const body = out.Body as AsyncIterable<Uint8Array> & { destroy?: () => void };
		if ((out.ContentLength ?? 0) > maxBytes) {
			body.destroy?.();
			throw new ObjectTooLarge(`${out.ContentLength} bytes over the ${maxBytes}-byte bound`);
		}
		const parts: Buffer[] = [];
		let total = 0;
		for await (const chunk of body) {
			total += chunk.length;
			if (total > maxBytes) {
				body.destroy?.();
				throw new ObjectTooLarge(`more than ${maxBytes} bytes`);
			}
			parts.push(Buffer.from(chunk));
		}
		return Buffer.concat(parts);
	}

	async putIfAbsent(key: string, bytes: Buffer, contentType: string): Promise<void> {
		if ((await this.size(key)) !== undefined) return;
		try {
			await this.client.send(
				new PutObjectCommand({
					Bucket: this.cfg.bucket,
					Key: key,
					Body: bytes,
					ContentType: contentType,
					IfNoneMatch: "*",
				}),
			);
		} catch (err) {
			// A content-addressed key that exists already holds these bytes.
			// MinIO answers a lost If-None-Match race with 412 and may close
			// the connection before the body is read, so an object that now
			// exists is the outcome either way.
			if (status(err) === 412 || (await this.size(key)) !== undefined) return;
			throw err;
		}
	}

	presignGet(key: string, ttlSeconds: number): Promise<string> {
		return getSignedUrl(this.signer, new GetObjectCommand({ Bucket: this.cfg.bucket, Key: key }), {
			expiresIn: ttlSeconds,
		});
	}

	presignPut(key: string, ttlSeconds: number): Promise<string> {
		return getSignedUrl(this.signer, new PutObjectCommand({ Bucket: this.cfg.bucket, Key: key }), {
			expiresIn: ttlSeconds,
		});
	}

	close(): void {
		this.client.destroy();
		this.signer.destroy();
	}
}

function status(err: unknown): number | undefined {
	return err instanceof S3ServiceException ? err.$metadata.httpStatusCode : undefined;
}

/** The in-memory store of the unit tests. */
export class MemoryObjects implements ObjectStore {
	readonly objects = new Map<string, Buffer>();
	async size(key: string): Promise<number | undefined> {
		return this.objects.get(key)?.length;
	}
	async read(key: string, maxBytes: number): Promise<Buffer> {
		const b = this.objects.get(key);
		if (!b) throw new ObjectMissing("object missing");
		if (b.length > maxBytes) throw new ObjectTooLarge(`${b.length} bytes over the ${maxBytes}-byte bound`);
		return b;
	}
	async putIfAbsent(key: string, bytes: Buffer): Promise<void> {
		if (!this.objects.has(key)) this.objects.set(key, Buffer.from(bytes));
	}
	async presignGet(key: string): Promise<string> {
		return `memory://get/${key}`;
	}
	async presignPut(key: string): Promise<string> {
		return `memory://put/${key}`;
	}
	close(): void {}
}
