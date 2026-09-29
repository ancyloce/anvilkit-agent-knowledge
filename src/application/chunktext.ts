// Chunk text is read only from the immutable parser result Knowledge
// accepted (content_ref parse/<launch-key>/result.json#<ordinal>), strictly
// parsed within a bound, and returned only when its digest equals the
// chunk's recorded content digest. Qdrant holds no text. One reader serves
// one operation; it caches the result objects it has read.

import type { ChunkRow } from "../adapters/ingestdb.js";
import { strictJson } from "../adapters/jobcontract.js";
import type { ObjectStore } from "../adapters/objects.js";
import type { ParseResult } from "../domain/parse.js";
import { digestOf } from "../domain/task.js";

/** A chunk whose stored text does not prove its recorded digest (or cannot be read). */
export class ChunkUnverified extends Error {}

export class ChunkTexts {
	private readonly results = new Map<string, Promise<ParseResult>>();

	constructor(
		private readonly objects: ObjectStore,
		private readonly maxResultBytes: number,
	) {}

	private result(key: string): Promise<ParseResult> {
		let p = this.results.get(key);
		if (!p) {
			p = this.objects.read(key, this.maxResultBytes).then((bytes) => strictJson(bytes) as ParseResult);
			this.results.set(key, p);
		}
		return p;
	}

	async text(chunk: Pick<ChunkRow, "chunkId" | "contentRef" | "contentDigest" | "ordinal">): Promise<string> {
		const hash = chunk.contentRef.lastIndexOf("#");
		const key = hash > 0 ? chunk.contentRef.slice(0, hash) : "";
		const ordinal = Number(chunk.contentRef.slice(hash + 1));
		if (!key.startsWith("parse/") || ordinal !== chunk.ordinal)
			throw new ChunkUnverified(`chunk ${chunk.chunkId}: content reference`);
		let r: ParseResult;
		try {
			r = await this.result(key);
		} catch (err) {
			this.results.delete(key);
			throw new ChunkUnverified(`chunk ${chunk.chunkId}: ${String(err).slice(0, 120)}`);
		}
		const c = r.chunks?.[ordinal];
		if (
			!c ||
			c.ordinal !== ordinal ||
			typeof c.text !== "string" ||
			digestOf(Buffer.from(c.text, "utf8")) !== chunk.contentDigest
		)
			throw new ChunkUnverified(`chunk ${chunk.chunkId}: digest`);
		return c.text;
	}
}
