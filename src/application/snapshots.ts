// SnapshotService (DD-07 §4): freezes exact source revisions and the index
// generation they are read from, for a brief. Creation requires every named
// source to be readable by the caller now and its current revision to be
// accepted in the accepted generation; the snapshot is idempotent by
// (tenant, command) and its request digest. A snapshot never freezes
// permission: a later read lists only what the caller can still read, and
// every search resolves the current ACL again.
import * as idx from "../adapters/indexdb.js";
import type * as db from "../adapters/postgres.js";
import * as rdb from "../adapters/retrievaldb.js";
import { RetrievalError, type Snapshot, snapshotDigest, snapshotIdOf } from "../domain/retrieval.js";
import { type Command, checkCommand, principalsOf, type Scope, SourceError } from "../domain/source.js";
import type { Logger } from "../log.js";

export class Snapshots {
	constructor(
		private readonly store: db.Store,
		private readonly log: Logger,
	) {}

	async create(cmd: Command, scope: Scope, sourceIds: string[]): Promise<{ snapshot: Snapshot; existing: boolean }> {
		try {
			checkCommand(cmd, scope);
		} catch (err) {
			if (err instanceof SourceError)
				throw new RetrievalError(err.code === "FORBIDDEN" ? "FORBIDDEN" : "INVALID_ARGUMENT", err.message);
			throw err;
		}
		const replay = async () => {
			const s = await rdb.getSnapshotByCommand(this.store.pool, cmd.tenantId, cmd.commandId);
			if (!s) return undefined;
			if (s.requestDigest !== cmd.requestDigest)
				throw new RetrievalError("COMMAND_CONFLICT", "the command id was used with another request");
			return { snapshot: this.visible(s, await this.readable(scope, s)), existing: true };
		};
		const replayed = await replay();
		if (replayed) return replayed;
		const ids = [...new Set(sourceIds)];
		try {
			const snapshot = await this.store.inTx(async (c) => {
				const gens = await idx.listGenerations(c);
				const g = gens.find((x) => x.state === "accepted");
				if (!g) throw new RetrievalError("NOT_INDEXED", "no accepted index generation");
				const readable = await rdb.readableNow(c, scope.tenantId, scope.projectId, [...principalsOf(scope)], ids);
				// An unreadable source is indistinguishable from a missing one.
				if (readable.size !== ids.length) throw new RetrievalError("NOT_FOUND", "source");
				const revisions = [...readable.values()].map((r) => ({ sourceId: r.sourceId, revision: r.currentRevision }));
				const accepted = await idx.acceptedEntries(c, g.generation, revisions);
				if (accepted.length !== revisions.length)
					throw new RetrievalError(
						"NOT_INDEXED",
						`${revisions.length - accepted.length} of ${revisions.length} sources are not indexed in generation ${g.generation}`,
					);
				const sources = [...readable.values()]
					.map((r) => ({ sourceId: r.sourceId, sourceRevision: r.currentRevision, contentDigest: r.contentDigest }))
					.sort((a, b) => (a.sourceId < b.sourceId ? -1 : 1));
				const s: Snapshot = {
					snapshotId: snapshotIdOf(cmd.tenantId, cmd.commandId),
					tenantId: scope.tenantId,
					projectId: scope.projectId,
					generation: g.generation,
					sources,
					contentDigest: snapshotDigest(g.generation, sources),
					requestDigest: cmd.requestDigest,
					createdAt: new Date(),
				};
				await rdb.insertSnapshot(c, s, cmd.commandId);
				return s;
			});
			this.log.info("snapshot created", {
				snapshotId: snapshot.snapshotId,
				generation: snapshot.generation,
				sources: snapshot.sources.length,
			});
			const stored = await rdb.getSnapshot(this.store.pool, snapshot.snapshotId);
			return { snapshot: stored ?? snapshot, existing: false };
		} catch (err) {
			// A concurrent identical command committed first: answer with it.
			if ((err as { code?: string }).code === "23505") {
				const again = await replay();
				if (again) return again;
			}
			throw err;
		}
	}

	/** The snapshot of the caller's tenant (and project) with only the sources the caller can read now. */
	async get(scope: Scope, snapshotId: string): Promise<Snapshot> {
		const s = await rdb.getSnapshot(this.store.pool, snapshotId);
		if (!s || s.tenantId !== scope.tenantId || (scope.projectId && s.projectId !== scope.projectId))
			throw new RetrievalError("NOT_FOUND", "snapshot");
		return this.visible(s, await this.readable(scope, s));
	}

	private readable(scope: Scope, s: Snapshot): Promise<Map<string, rdb.Readable>> {
		return rdb.readableNow(
			this.store.pool,
			scope.tenantId,
			scope.projectId,
			[...principalsOf(scope)],
			s.sources.map((x) => x.sourceId),
		);
	}

	/** Revoked or deleted sources are left out of what a read discloses; the frozen digest is unchanged. */
	private visible(s: Snapshot, readable: Map<string, rdb.Readable>): Snapshot {
		return { ...s, sources: s.sources.filter((x) => readable.has(x.sourceId)) };
	}
}
