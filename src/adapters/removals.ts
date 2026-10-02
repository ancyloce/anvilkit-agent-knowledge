// The removal inventory (P23, contracts.md §3, platform.md §6): Knowledge's
// immutable record of every memory deletion and revocation, kept outside
// the business database in the independent DR store (its own bucket, user
// and bucket-limited policy). A point-in-time restore of anvilkit_knowledge
// cannot erase a record, so the removals a restore lost are found again
// here and re-applied before memory serves. Records are created only when
// absent and never changed or deleted; a key that exists already must hold
// the same bytes. A listing is complete or it fails: a truncated page
// without a continuation token is an error, never an empty remainder.
// Records carry identities and digests only, never fact content; no key,
// credential or record body is logged.
import {
	GetObjectCommand,
	type GetObjectCommandOutput,
	ListObjectsV2Command,
	NoSuchKey,
	PutObjectCommand,
	S3Client,
	S3ServiceException,
} from "@aws-sdk/client-s3";

export interface RemovalInventoryConfig {
	endpoint: string;
	bucket: string;
	region: string;
	accessKeyId: string;
	secretAccessKey: string;
	timeoutMs: number;
}

/** The key exists and holds other bytes: an integrity failure, never overwritten. */
export class RemovalConflict extends Error {}
/** The create's outcome is unknown (the store did not answer or the read-back failed). */
export class RemovalUncertain extends Error {}
/** The listing could not be completed. */
export class RemovalListIncomplete extends Error {}
/** A record over the read bound: never a removal record. */
export class RemovalTooLarge extends Error {}

export interface RemovalPage {
	keys: string[];
	/** Present when more keys follow; pass it back to read the next page. */
	next?: string;
}

/** The port the removal use case depends on (tests use the in-memory one). */
export interface RemovalInventory {
	/** Creates the record when absent; an existing record must hold exactly these bytes. */
	create(key: string, body: Buffer): Promise<void>;
	/** The record's bytes, or undefined when absent; over maxBytes is RemovalTooLarge. */
	read(key: string, maxBytes: number): Promise<Buffer | undefined>;
	/** One page of the keys under prefix after startAfter, in key order. */
	list(prefix: string, startAfter: string, next: string | undefined, maxKeys: number): Promise<RemovalPage>;
	close(): void;
}

const readBackAttempts = 3;

function status(err: unknown): number | undefined {
	return err instanceof S3ServiceException ? err.$metadata.httpStatusCode : undefined;
}

export class S3RemovalInventory implements RemovalInventory {
	private readonly client: S3Client;

	constructor(private readonly cfg: RemovalInventoryConfig) {
		this.client = new S3Client({
			endpoint: cfg.endpoint,
			region: cfg.region,
			forcePathStyle: true,
			credentials: { accessKeyId: cfg.accessKeyId, secretAccessKey: cfg.secretAccessKey },
			maxAttempts: 1,
		});
	}

	private signal(): AbortSignal {
		return AbortSignal.timeout(this.cfg.timeoutMs);
	}

	async create(key: string, body: Buffer): Promise<void> {
		let failure: unknown;
		try {
			await this.client.send(
				new PutObjectCommand({
					Bucket: this.cfg.bucket,
					Key: key,
					Body: body,
					ContentType: "application/json",
					IfNoneMatch: "*",
				}),
				{ abortSignal: this.signal() },
			);
			return;
		} catch (err) {
			failure = err;
		}
		// A lost race (412/409), a lost answer or a closed connection: the
		// record that exists now decides. MinIO answers a lost If-None-Match
		// race with 412 and closes the connection, so the first read-back may
		// meet the closed socket: it is read again before the write is
		// called uncertain.
		let existing: Buffer | undefined;
		for (let attempt = 1; ; attempt++) {
			try {
				existing = await this.read(key, body.length);
				break;
			} catch (err) {
				if (err instanceof RemovalTooLarge)
					throw new RemovalConflict("a removal record with other bytes exists under its key");
				if (attempt < readBackAttempts) continue;
				throw new RemovalUncertain(
					`create not confirmed (${describe(failure)}); read-back failed (${err instanceof Error ? err.name : String(err)})`,
				);
			}
		}
		if (existing === undefined) throw new RemovalUncertain(`create not confirmed (${describe(failure)})`);
		if (!existing.equals(body)) throw new RemovalConflict("a removal record with other bytes exists under its key");
	}

	async read(key: string, maxBytes: number): Promise<Buffer | undefined> {
		let out: GetObjectCommandOutput;
		try {
			out = await this.client.send(new GetObjectCommand({ Bucket: this.cfg.bucket, Key: key }), {
				abortSignal: this.signal(),
			});
		} catch (err) {
			if (err instanceof NoSuchKey || status(err) === 404) return undefined;
			throw err;
		}
		const body = out.Body as AsyncIterable<Uint8Array> & { destroy?: () => void };
		if ((out.ContentLength ?? 0) > maxBytes) {
			body.destroy?.();
			throw new RemovalTooLarge(`${out.ContentLength} bytes over the ${maxBytes}-byte bound`);
		}
		const parts: Buffer[] = [];
		let total = 0;
		for await (const chunk of body) {
			total += chunk.length;
			if (total > maxBytes) {
				body.destroy?.();
				throw new RemovalTooLarge(`more than ${maxBytes} bytes`);
			}
			parts.push(Buffer.from(chunk));
		}
		return Buffer.concat(parts);
	}

	async list(prefix: string, startAfter: string, next: string | undefined, maxKeys: number): Promise<RemovalPage> {
		let out: { Contents?: { Key?: string }[]; IsTruncated?: boolean; NextContinuationToken?: string };
		try {
			out = await this.client.send(
				new ListObjectsV2Command({
					Bucket: this.cfg.bucket,
					Prefix: prefix,
					StartAfter: next ? undefined : startAfter || undefined,
					ContinuationToken: next,
					MaxKeys: maxKeys,
				}),
				{ abortSignal: this.signal() },
			);
		} catch (err) {
			throw new RemovalListIncomplete(`listing failed: ${describe(err)}`);
		}
		const keys: string[] = [];
		for (const c of out.Contents ?? []) {
			if (!c.Key) throw new RemovalListIncomplete("a listed entry has no key");
			keys.push(c.Key);
		}
		if (out.IsTruncated && !out.NextContinuationToken)
			throw new RemovalListIncomplete("a truncated page without a continuation token");
		return { keys, next: out.IsTruncated ? out.NextContinuationToken : undefined };
	}

	close(): void {
		this.client.destroy();
	}
}

function describe(err: unknown): string {
	const code = status(err);
	const name = err instanceof Error ? err.name : "error";
	return code ? `${name} ${code}` : name;
}

/** The in-memory inventory of the unit tests, with fault injection. */
export class MemoryRemovalInventory implements RemovalInventory {
	readonly records = new Map<string, Buffer>();
	/** Fails the next n creates before (lost: nothing written) or after (written, answer lost) the write. */
	failCreates = { before: 0, after: 0 };
	/** Lets this many more pages list, then fails every listing until reset (undefined: never fails). */
	failListAfter: number | undefined;

	async create(key: string, body: Buffer): Promise<void> {
		if (this.failCreates.before > 0) {
			this.failCreates.before--;
			throw new RemovalUncertain("injected: the store did not answer");
		}
		const existing = this.records.get(key);
		if (existing && !existing.equals(body)) throw new RemovalConflict("a removal record with other bytes exists");
		if (!existing) this.records.set(key, Buffer.from(body));
		if (this.failCreates.after > 0) {
			this.failCreates.after--;
			throw new RemovalUncertain("injected: the answer was lost");
		}
	}

	async read(key: string, maxBytes: number): Promise<Buffer | undefined> {
		const b = this.records.get(key);
		if (b && b.length > maxBytes) throw new RemovalTooLarge(`${b.length} bytes over the ${maxBytes}-byte bound`);
		return b;
	}

	async list(prefix: string, startAfter: string, next: string | undefined, maxKeys: number): Promise<RemovalPage> {
		if (this.failListAfter !== undefined) {
			if (this.failListAfter <= 0) throw new RemovalListIncomplete("injected: listing failed");
			this.failListAfter--;
		}
		const after = next ?? startAfter;
		const keys = [...this.records.keys()].filter((k) => k.startsWith(prefix) && k > after).sort();
		const out = keys.slice(0, maxKeys);
		return { keys: out, next: keys.length > maxKeys ? out[out.length - 1] : undefined };
	}

	resetFaults(): void {
		this.failCreates = { before: 0, after: 0 };
		this.failListAfter = undefined;
	}

	close(): void {}
}
