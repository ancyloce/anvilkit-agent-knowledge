// In-memory doubles of the object store and the removal inventory for the
// unit tests (the production adapters are S3Objects and S3RemovalInventory).
import {
	ObjectMissing,
	type ObjectStore,
	ObjectTooLarge,
	type PresignedUpload,
	type UploadBinding,
} from "../src/adapters/objects.js";
import {
	RemovalConflict,
	type RemovalInventory,
	RemovalListIncomplete,
	type RemovalPage,
	RemovalTooLarge,
	RemovalUncertain,
} from "../src/adapters/removals.js";

/** The in-memory object store of the unit tests. */
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
	/** The presigned uploads handed out, by key (tests perform the PUT themselves). */
	readonly uploads = new Map<string, UploadBinding & { ttlSeconds: number }>();
	async presignUpload(key: string, ttlSeconds: number, b: UploadBinding): Promise<PresignedUpload> {
		this.uploads.set(key, { ...b, ttlSeconds });
		return {
			url: `memory://upload/${key}`,
			headers: {
				"content-length": String(b.sizeBytes),
				"content-type": b.mediaType,
				"x-amz-checksum-sha256": b.sha256Base64,
			},
		};
	}
	async delete(key: string): Promise<void> {
		this.objects.delete(key);
	}
	close(): void {}
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
