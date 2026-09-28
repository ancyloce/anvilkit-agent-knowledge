// The Source Registry's commands (DD-07 §2), one transaction each. A
// registration first reads the caller's upload within the bound, verifies
// its size, digest and media type against the actual bytes and copies it
// to the content-addressed key (no locks held); the transaction then
// commits the source, its revision, the trusted ACL snapshot, the command
// record, the ingest request and the knowledge-ingest background request
// with its event. ACL replacement and deletion are compare-and-set on the
// current revisions; both append an ACL revision (deletion one without
// entries) and publish source.authorization-revoked when access shrinks.
// Every command is idempotent by (tenant, command) and its request digest.
import * as idb from "../adapters/ingestdb.js";
import { ObjectMissing, type ObjectStore, ObjectTooLarge } from "../adapters/objects.js";
import * as db from "../adapters/postgres.js";
import { sourceRevokedEvent } from "../domain/event.js";
import {
	type AccessEntry,
	type Command,
	checkCommand,
	checkMediaType,
	type IngestInput,
	ingestProfile,
	ingestRequestIdOf,
	ingestTaskIdOf,
	principalsOf,
	readable,
	revokes,
	type Scope,
	type Source,
	SourceError,
	type SourceKind,
	sourceIdOf,
	sourceObjectKeyOf,
	trustedAcl,
	uploadKeyOf,
} from "../domain/source.js";
import { digestOf as sourceDigest } from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Clock, Tasks } from "./tasks.js";

/** The reviewed parser profile ingestion freezes into each request (contracts/jobs parser profile). */
export interface IngestPlan {
	profileId: string;
	profileRevision: number;
	chunkerId: string;
	chunkerRevision: number;
	mediaTypes: readonly string[];
	maxInputBytes: number;
}

export interface SourceBounds {
	maxSourceBytes: number;
}

export interface Registered {
	source: Source;
	ingestRequestId: string;
	existing: boolean;
}

export class Sources {
	constructor(
		private readonly store: db.Store,
		private readonly tasks: Tasks,
		private readonly objects: () => ObjectStore | undefined,
		private readonly plan: () => IngestPlan | undefined,
		private readonly bounds: () => SourceBounds,
		private readonly clock: Clock,
		private readonly log: Logger,
	) {}

	private async replay(cmd: Command, kind: idb.CommandRecord["commandKind"]): Promise<Source | undefined> {
		const rec = await idb.getCommand(this.store.pool, cmd.tenantId, cmd.commandId);
		if (!rec) return undefined;
		if (rec.commandKind !== kind || rec.requestDigest !== cmd.requestDigest)
			throw new SourceError("COMMAND_CONFLICT", "the command id was used with another request");
		const s = await idb.getSource(this.store.pool, rec.sourceId);
		if (!s) throw new SourceError("NOT_FOUND", "source");
		return s;
	}

	async register(
		cmd: Command,
		scope: Scope,
		req: { kind: SourceKind; locator: string; contentDigest: string; mediaType: string; sizeBytes: string },
		access: AccessEntry[],
	): Promise<Registered> {
		checkCommand(cmd, scope);
		const acl = trustedAcl(scope, access);
		const replayed = await this.replay(cmd, "register");
		if (replayed) return { source: replayed, ingestRequestId: ingestRequestIdOf(replayed.sourceId, 1), existing: true };
		if (req.kind !== "document")
			throw new SourceError(
				"UNSUPPORTED_SOURCE",
				`${req.kind} sources are not ingested in this build (no crawler, repository or brand reader)`,
			);
		const plan = this.plan();
		const objects = this.objects();
		if (!plan || !objects)
			throw new SourceError("UNSUPPORTED_SOURCE", "no qualified parser profile or object store is configured");
		if (!plan.mediaTypes.includes(req.mediaType))
			throw new SourceError("UNSUPPORTED_SOURCE", `media type ${req.mediaType} is not in ${plan.profileId}`);
		const declared = Number(req.sizeBytes);
		const limit = Math.min(this.bounds().maxSourceBytes, plan.maxInputBytes);
		if (!Number.isSafeInteger(declared) || declared > limit)
			throw new SourceError("SOURCE_TOO_LARGE", `${req.sizeBytes} bytes over the ${limit}-byte bound`);

		// The actual bytes, not the caller's description, identify the revision.
		let bytes: Buffer;
		try {
			bytes = await objects.read(uploadKeyOf(scope, req.locator), limit);
		} catch (err) {
			if (err instanceof ObjectTooLarge) throw new SourceError("SOURCE_TOO_LARGE", err.message);
			if (err instanceof ObjectMissing) throw new SourceError("SOURCE_UNVERIFIED", "the upload does not exist");
			throw err;
		}
		if (bytes.length !== declared || sourceDigest(bytes) !== req.contentDigest)
			throw new SourceError("SOURCE_UNVERIFIED", "size or content digest differs from the uploaded bytes");
		const mediaType = checkMediaType(bytes, req.mediaType);
		const sourceId = sourceIdOf(cmd);
		const objectKey = sourceObjectKeyOf(scope.tenantId, req.contentDigest);
		await objects.putIfAbsent(objectKey, bytes, mediaType);

		const requestId = ingestRequestIdOf(sourceId, 1);
		const taskId = ingestTaskIdOf(sourceId, 1);
		const input: IngestInput = {
			schemaVersion: 1,
			computation: ingestProfile,
			sourceId,
			sourceRevision: 1,
			ingestRequestId: requestId,
			parserProfile: plan.profileId,
			parserProfileRevision: plan.profileRevision,
			input: { digest: req.contentDigest, sizeBytes: String(bytes.length), mediaType },
		};
		try {
			await this.store.inTx(async (c) => {
				if (await idb.getCommand(c, cmd.tenantId, cmd.commandId))
					throw new SourceError("COMMAND_CONFLICT", "concurrent registration");
				await idb.insertSource(c, {
					sourceId,
					tenantId: scope.tenantId,
					projectId: scope.projectId,
					kind: "document",
					locator: req.locator,
					commandId: cmd.commandId,
					requestDigest: cmd.requestDigest,
					contentDigest: req.contentDigest,
					mediaType,
					sizeBytes: bytes.length,
					objectKey,
				});
				await idb.insertAcl(c, sourceId, 1, acl, cmd.actorId, cmd.commandId);
				await idb.insertCommand(c, cmd.tenantId, cmd.commandId, {
					commandKind: "register",
					requestDigest: cmd.requestDigest,
					sourceId,
				});
				await idb.insertIngestRequest(c, {
					requestId,
					sourceId,
					sourceRevision: 1,
					state: "pending",
					parserProfile: plan.profileId,
					parserProfileRevision: plan.profileRevision,
					chunkerProfile: plan.chunkerId,
					chunkerRevision: plan.chunkerRevision,
					taskId,
					failureCode: "",
					pageCount: null,
					chunkCount: null,
					resultRef: "",
					resultDigest: "",
				});
				await this.tasks.requestIn(c, {
					taskId,
					tenantId: scope.tenantId,
					kind: "knowledge-ingest",
					profile: ingestProfile,
					input: JSON.stringify(input),
					effects: "reconstructible",
					dispatchId: "",
					authorizationRef: `source:${sourceId}`,
					correlationId: cmd.commandId,
				});
			});
		} catch (err) {
			// A concurrent identical command committed first: answer with it.
			if (isUniqueViolation(err) || (err instanceof SourceError && err.code === "COMMAND_CONFLICT")) {
				const again = await this.replay(cmd, "register");
				if (again) return { source: again, ingestRequestId: requestId, existing: true };
			}
			throw err;
		}
		const source = await idb.getSource(this.store.pool, sourceId);
		if (!source) throw new SourceError("NOT_FOUND", "source vanished after commit");
		this.log.info("source registered", { sourceId, ingestRequestId: requestId, taskId });
		return { source, ingestRequestId: requestId, existing: false };
	}

	async get(scope: Scope, sourceId: string): Promise<Source> {
		const s = await idb.getSource(this.store.pool, sourceId);
		// An unreadable source is indistinguishable from a missing one.
		if (!s || !readable(s, scope)) throw new SourceError("NOT_FOUND", "source");
		return s;
	}

	async list(scope: Scope, cursor: string, limit: number): Promise<{ sources: Source[]; nextCursor: string }> {
		const pageSize = limit === 0 ? 50 : limit;
		let after = "";
		if (cursor) {
			after = Buffer.from(cursor, "base64url").toString("utf8");
			if (!/^src-[0-9a-f]{32}$/.test(after)) throw new SourceError("INVALID_ARGUMENT", "cursor");
		}
		const rows = await idb.listReadable(
			this.store.pool,
			scope.tenantId,
			scope.projectId,
			[...principalsOf(scope)],
			after,
			pageSize + 1,
		);
		const page = rows.slice(0, pageSize);
		const last = page[page.length - 1];
		return {
			sources: page,
			nextCursor: rows.length > pageSize && last ? Buffer.from(last.sourceId).toString("base64url") : "",
		};
	}

	async updateAccess(
		cmd: Command,
		scope: Scope,
		sourceId: string,
		expectedAclRevision: number,
		access: AccessEntry[],
	): Promise<{ source: Source; existing: boolean }> {
		checkCommand(cmd, scope);
		const acl = trustedAcl(scope, access);
		const replayed = await this.replay(cmd, "update_access");
		if (replayed) {
			if (replayed.sourceId !== sourceId) throw new SourceError("COMMAND_CONFLICT", "another source");
			return { source: replayed, existing: true };
		}
		await this.store.inTx(async (c) => {
			if (await idb.getCommand(c, cmd.tenantId, cmd.commandId))
				throw new SourceError("COMMAND_CONFLICT", "concurrent command");
			const s = await idb.getSource(c, sourceId, true);
			if (!s || !readable(s, scope)) throw new SourceError("NOT_FOUND", "source");
			if (s.aclRevision !== expectedAclRevision)
				throw new SourceError("REVISION_MISMATCH", `ACL revision is ${s.aclRevision}`);
			const next = s.aclRevision + 1;
			await idb.insertAcl(c, sourceId, next, acl, cmd.actorId, cmd.commandId);
			await idb.setAclRevision(c, sourceId, next);
			await idb.insertCommand(c, cmd.tenantId, cmd.commandId, {
				commandKind: "update_access",
				requestDigest: cmd.requestDigest,
				sourceId,
			});
			if (revokes(s.access, acl))
				await db.publishOutbox(
					c,
					sourceRevokedEvent(
						{ sourceId, tenantId: s.tenantId, aclRevision: next, revision: next },
						cmd.commandId,
						this.clock.now(),
					),
				);
		});
		const source = await idb.getSource(this.store.pool, sourceId);
		if (!source) throw new SourceError("NOT_FOUND", "source");
		return { source, existing: false };
	}

	async delete(
		cmd: Command,
		scope: Scope,
		sourceId: string,
		expectedRevision: number,
	): Promise<{ source: Source; existing: boolean }> {
		checkCommand(cmd, scope);
		const replayed = await this.replay(cmd, "delete");
		if (replayed) {
			if (replayed.sourceId !== sourceId) throw new SourceError("COMMAND_CONFLICT", "another source");
			return { source: replayed, existing: true };
		}
		await this.store.inTx(async (c) => {
			if (await idb.getCommand(c, cmd.tenantId, cmd.commandId))
				throw new SourceError("COMMAND_CONFLICT", "concurrent command");
			const s = await idb.getSource(c, sourceId, true);
			if (!s || !readable(s, scope)) throw new SourceError("NOT_FOUND", "source");
			if (s.currentRevision !== expectedRevision)
				throw new SourceError("REVISION_MISMATCH", `source revision is ${s.currentRevision}`);
			// Readability ends in this transaction: the deleted flag and an
			// ACL revision without entries; open parses are canceled with it.
			const next = s.aclRevision + 1;
			await idb.insertAcl(c, sourceId, next, [], cmd.actorId, cmd.commandId);
			await idb.setAclRevision(c, sourceId, next);
			await idb.markDeleted(c, sourceId);
			for (const taskId of await idb.openIngestTasks(c, sourceId)) await this.tasks.cancelIn(c, taskId);
			await idb.insertCommand(c, cmd.tenantId, cmd.commandId, {
				commandKind: "delete",
				requestDigest: cmd.requestDigest,
				sourceId,
			});
			await db.publishOutbox(
				c,
				sourceRevokedEvent(
					{ sourceId, tenantId: s.tenantId, aclRevision: next, revision: next },
					cmd.commandId,
					this.clock.now(),
				),
			);
		});
		const source = await idb.getSource(this.store.pool, sourceId);
		if (!source) throw new SourceError("NOT_FOUND", "source");
		this.log.info("source deleted", { sourceId });
		return { source, existing: false };
	}
}

function isUniqueViolation(err: unknown): boolean {
	return typeof err === "object" && err !== null && (err as { code?: string }).code === "23505";
}
