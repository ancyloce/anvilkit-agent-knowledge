// The Source Registry's commands (DD-07 §2), one transaction each. A
// registration first reads the caller's upload within the bound, verifies
// its size, digest and media type against the actual bytes and copies it
// to the content-addressed key (no locks held); the transaction then
// commits the source, its revision, the trusted ACL snapshot, the command
// record, the ingest request and the knowledge-ingest background request
// with its event. ACL replacement and deletion are compare-and-set on the
// current revisions; both append an ACL revision (deletion one without
// entries); a shrinking ACL is enforced by the live read gate and recorded
// in the removal inventory (source.authorization-revoked left the event
// catalog in P2.8: it had no consumer).
// Every command is idempotent by (tenant, command) and its request digest.
import * as idx from "../adapters/indexdb.js";
import * as idb from "../adapters/ingestdb.js";
import * as mdb from "../adapters/memorydb.js";
import { ObjectMissing, type ObjectStore, ObjectTooLarge } from "../adapters/objects.js";
import type * as db from "../adapters/postgres.js";
import { type SourceRemovalRecord, sourceRemovalKeyOf } from "../domain/removal.js";
import {
	type AccessEntry,
	type Command,
	checkCommand,
	checkMediaType,
	type IngestInput,
	ingestProfile,
	ingestRequestIdOf,
	ingestTaskIdOf,
	isUploadId,
	type MediaType,
	manageable,
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
	uploadIdOf,
	uploadKeyOf,
	uploadNameOf,
} from "../domain/source.js";
import { digestOf as sourceDigest } from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Removals, RestoreOutcome } from "./removals.js";
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
	/** P2.6: how long an upload capability may be used (PUT and registration). */
	uploadTtlMs: number;
	/**
	 * DEVELOPMENT_ONLY (development.enabled): a document may name an upload
	 * written into Knowledge's bucket by a trusted fixture (upload:<name>)
	 * without a BeginUpload capability. Off: every document names one.
	 */
	legacyUploads: boolean;
}

/** P2.6: the capability BeginUpload answers. */
export interface UploadCapability {
	uploadId: string;
	url: string;
	method: "PUT";
	headers: Record<string, string>;
	expiresAt: Date;
	existing: boolean;
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

	/**
	 * P2.6: a single-use upload capability for one document of the declared
	 * digest, size and media type, for this actor of this tenant. Same
	 * command, same declaration: the same upload, a fresh signature until it
	 * expires; another declaration under the command is a conflict.
	 */
	async beginUpload(
		cmd: Command,
		scope: Scope,
		req: { contentDigest: string; sizeBytes: string; mediaType: string },
	): Promise<UploadCapability> {
		checkCommand(cmd, scope);
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
		if (!/^sha256:[0-9a-f]{64}$/.test(req.contentDigest)) throw new SourceError("INVALID_ARGUMENT", "content digest");
		const now = this.clock.now();
		const uploadId = uploadIdOf(cmd);
		const { record: rec, inserted } = await idb.insertUpload(this.store.pool, {
			uploadId,
			tenantId: scope.tenantId,
			actorId: scope.actorId,
			commandId: cmd.commandId,
			requestDigest: cmd.requestDigest,
			contentDigest: req.contentDigest,
			sizeBytes: declared,
			mediaType: req.mediaType,
			objectKey: `uploads/${scope.tenantId}/${uploadId}`,
			expiresAt: new Date(now.getTime() + this.bounds().uploadTtlMs),
		});
		const existing = !inserted;
		if (rec.requestDigest !== cmd.requestDigest)
			throw new SourceError("COMMAND_CONFLICT", "the command id was used with another upload");
		if (rec.consumedAt) throw new SourceError("SOURCE_UNVERIFIED", "the upload was already registered");
		const ttl = Math.floor((rec.expiresAt.getTime() - now.getTime()) / 1000);
		if (ttl < 1) throw new SourceError("SOURCE_UNVERIFIED", "the upload capability expired");
		const signed = await objects.presignUpload(rec.objectKey, ttl, {
			sizeBytes: rec.sizeBytes,
			mediaType: rec.mediaType,
			sha256Base64: Buffer.from(rec.contentDigest.slice("sha256:".length), "hex").toString("base64"),
		});
		this.log.info("source upload capability issued", { uploadId, expiresAt: rec.expiresAt.toISOString(), existing });
		return { uploadId, url: signed.url, method: "PUT", headers: signed.headers, expiresAt: rec.expiresAt, existing };
	}

	/**
	 * P2.6: the upload a document names, checked against its capability: the
	 * same actor of the same tenant, before it expires, not yet consumed, the
	 * same declaration. A name that is no upload id is a fixture's direct
	 * upload, admitted only behind the development guard.
	 */
	private async capabilityFor(
		scope: Scope,
		locator: string,
		req: { contentDigest: string; mediaType: string },
		declared: number,
	): Promise<idb.UploadRecord | undefined> {
		const name = uploadNameOf(locator);
		if (!isUploadId(name)) {
			if (this.bounds().legacyUploads) return undefined;
			throw new SourceError(
				"SOURCE_UNVERIFIED",
				"a document names an upload capability (upload:<uploadId> of BeginUpload)",
			);
		}
		const u = await idb.getUpload(this.store.pool, scope.tenantId, name);
		if (!u) throw new SourceError("SOURCE_UNVERIFIED", "no such upload");
		if (u.actorId !== scope.actorId)
			throw new SourceError("SOURCE_UNVERIFIED", "the upload capability belongs to another actor");
		if (u.consumedAt) throw new SourceError("SOURCE_UNVERIFIED", "the upload was already registered");
		if (this.clock.now().getTime() >= u.expiresAt.getTime())
			throw new SourceError("SOURCE_UNVERIFIED", "the upload capability expired");
		if (u.sizeBytes !== declared || u.contentDigest !== req.contentDigest || u.mediaType !== req.mediaType)
			throw new SourceError("SOURCE_UNVERIFIED", "the registration differs from the upload's declaration");
		return u;
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

		const capability = await this.capabilityFor(scope, req.locator, req, declared);
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
					ownerId: scope.actorId,
				});
				if (
					capability &&
					!(await idb.consumeUpload(c, scope.tenantId, capability.uploadId, sourceId, this.clock.now()))
				)
					throw new SourceError("SOURCE_UNVERIFIED", "the upload was already registered");
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

	private removals: Removals | undefined;

	/** P2.7: deletions and revocations are recorded in the removal inventory; reads close until it is reconciled. */
	setRemovals(r: Removals): void {
		this.removals = r;
	}

	private gate(): void {
		if (this.removals && !this.removals.ready())
			throw new SourceError("UNAVAILABLE", "sources are closed until the removal inventory is reconciled");
	}

	/** Commits the removal row of a deletion or revocation in the caller's transaction (pending until recorded). */
	private async removalIn(
		c: db.PoolClient,
		cmd: Command,
		sourceId: string,
		decision: "delete" | "revoke",
		aclRevision: number,
		access: AccessEntry[],
	): Promise<{ record: SourceRemovalRecord; key: string }> {
		const record: SourceRemovalRecord = {
			schemaVersion: 1,
			kind: "source-removal",
			tenantId: cmd.tenantId,
			commandId: cmd.commandId,
			requestDigest: cmd.requestDigest,
			sourceId,
			decision,
			decider: cmd.actorId,
			aclRevision,
			access: decision === "delete" ? [] : idb.accessTextOf(access),
			recordedAt: (await mdb.removalTime(c)).toISOString(),
		};
		const key = sourceRemovalKeyOf(await mdb.removalScope(c), record);
		await idb.insertSourceRemoval(c, record, key);
		return { record, key };
	}

	/** The committed removal's record is in the inventory before the caller is answered. */
	private async recordRemoval(removal: { record: SourceRemovalRecord; key: string }): Promise<void> {
		if (!this.removals) return;
		try {
			await this.removals.record(removal.record, removal.key);
		} catch {
			throw new SourceError(
				"UNAVAILABLE",
				"the removal is committed; its record is not confirmed in the removal inventory yet: retry the command",
			);
		}
	}

	/**
	 * P2.7: re-applies a source deletion or revocation that a restore erased,
	 * under its original command: a deletion deletes again (empty ACL
	 * revision, cancelled ingest), a revocation installs the recorded access
	 * list again as the next ACL revision. A command the database knows, or a
	 * source it no longer holds, needs nothing.
	 */
	async applyRestoredSource(r: SourceRemovalRecord, key: string): Promise<RestoreOutcome> {
		return this.store.inTx(async (c) => {
			if (await idb.getCommand(c, r.tenantId, r.commandId)) return "known";
			const s = await idb.getSource(c, r.sourceId, true);
			if (!s || s.tenantId !== r.tenantId) return "absent";
			const next = s.aclRevision + 1;
			if (r.decision === "delete") {
				if (!s.deleted) {
					await idb.insertAcl(c, s.sourceId, next, [], r.decider, r.commandId);
					await idb.setAclRevision(c, s.sourceId, next);
					await idb.markDeleted(c, s.sourceId);
				}
			} else {
				await idb.insertAcl(c, s.sourceId, next, idb.accessOfText(r.access), r.decider, r.commandId);
				await idb.setAclRevision(c, s.sourceId, next);
			}
			await idb.insertCommand(c, r.tenantId, r.commandId, {
				commandKind: r.decision === "delete" ? "delete" : "update_access",
				requestDigest: r.requestDigest,
				sourceId: s.sourceId,
			});
			await idb.insertSourceRemoval(c, r, key, "restored");
			this.log.warn("a source removal a restore erased was re-applied", { sourceId: s.sourceId, decision: r.decision });
			return "restored";
		});
	}

	/**
	 * P2.7: the re-ingest of a parser or chunker change: every live current
	 * revision indexed under another chunker is parsed again under the
	 * current plan (its own ingest request and knowledge-ingest task, the
	 * revision's verified bytes), so the generation of the new chunker can
	 * index it before it is accepted. Answers how many it requested.
	 */
	async requestReparse(limit = 50): Promise<number> {
		const plan = this.plan();
		if (!plan) return 0;
		const candidates = await idx.reparseCandidates(
			this.store.pool,
			{
				parserProfile: plan.profileId,
				parserProfileRevision: plan.profileRevision,
				chunkerProfile: plan.chunkerId,
				chunkerRevision: plan.chunkerRevision,
			},
			limit,
		);
		let n = 0;
		for (const r of candidates) {
			const suffix = `p${plan.profileRevision}-c${plan.chunkerRevision}`;
			const requestId = `${ingestRequestIdOf(r.sourceId, r.revision)}-${suffix}`;
			const taskId = `${ingestTaskIdOf(r.sourceId, r.revision)}-${suffix}`;
			const input: IngestInput = {
				schemaVersion: 1,
				computation: ingestProfile,
				sourceId: r.sourceId,
				sourceRevision: r.revision,
				ingestRequestId: requestId,
				parserProfile: plan.profileId,
				parserProfileRevision: plan.profileRevision,
				input: { digest: r.contentDigest, sizeBytes: String(r.sizeBytes), mediaType: r.mediaType as MediaType },
			};
			try {
				await this.store.inTx(async (c) => {
					await idb.insertIngestRequest(c, {
						requestId,
						sourceId: r.sourceId,
						sourceRevision: r.revision,
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
						tenantId: r.tenantId,
						kind: "knowledge-ingest",
						profile: ingestProfile,
						input: JSON.stringify(input),
						effects: "reconstructible",
						dispatchId: "",
						authorizationRef: `source:${r.sourceId}`,
						correlationId: `reparse-${suffix}`,
					});
				});
				n++;
			} catch (err) {
				// Another replica requested it first.
				if (!isUniqueViolation(err)) throw err;
			}
		}
		if (n > 0) this.log.info("re-ingest requested for a parser or chunker change", { requests: n });
		return n;
	}

	async get(scope: Scope, sourceId: string): Promise<Source> {
		this.gate();
		const s = await idb.getSource(this.store.pool, sourceId);
		// An unreadable source is indistinguishable from a missing one.
		if (!s || !readable(s, scope)) throw new SourceError("NOT_FOUND", "source");
		return s;
	}

	async list(scope: Scope, cursor: string, limit: number): Promise<{ sources: Source[]; nextCursor: string }> {
		this.gate();
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
			await this.ensureRecorded(cmd);
			return { source: replayed, existing: true };
		}
		let removal: { record: SourceRemovalRecord; key: string } | undefined;
		await this.store.inTx(async (c) => {
			if (await idb.getCommand(c, cmd.tenantId, cmd.commandId))
				throw new SourceError("COMMAND_CONFLICT", "concurrent command");
			const s = await idb.getSource(c, sourceId, true);
			if (!s || !readable(s, scope)) throw new SourceError("NOT_FOUND", "source");
			// P2.7: reading is not managing.
			if (!manageable(s, scope))
				throw new SourceError("FORBIDDEN", "replacing the access list needs the owner or a manage entry");
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
			if (revokes(s.access, acl)) {
				// P2.7: a revocation survives a restore (removal inventory).
				removal = await this.removalIn(c, cmd, sourceId, "revoke", next, acl);
			}
		});
		if (removal) await this.recordRemoval(removal);
		const source = await idb.getSource(this.store.pool, sourceId);
		if (!source) throw new SourceError("NOT_FOUND", "source");
		return { source, existing: false };
	}

	/** A replayed deletion or revocation completes its inventory record if it is still pending. */
	private async ensureRecorded(cmd: Command): Promise<void> {
		if (!this.removals) return;
		try {
			await this.removals.ensureSourceRecorded(cmd.tenantId, cmd.commandId);
		} catch {
			throw new SourceError(
				"UNAVAILABLE",
				"the removal is committed; its record is not confirmed in the removal inventory yet: retry the command",
			);
		}
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
			await this.ensureRecorded(cmd);
			return { source: replayed, existing: true };
		}
		let removal: { record: SourceRemovalRecord; key: string } | undefined;
		await this.store.inTx(async (c) => {
			if (await idb.getCommand(c, cmd.tenantId, cmd.commandId))
				throw new SourceError("COMMAND_CONFLICT", "concurrent command");
			const s = await idb.getSource(c, sourceId, true);
			if (!s || !readable(s, scope)) throw new SourceError("NOT_FOUND", "source");
			// P2.7: reading is not managing.
			if (!manageable(s, scope)) throw new SourceError("FORBIDDEN", "deleting needs the owner or a manage entry");
			if (s.currentRevision !== expectedRevision)
				throw new SourceError("REVISION_MISMATCH", `source revision is ${s.currentRevision}`);
			// Readability ends in this transaction: the deleted flag and an
			// ACL revision without entries; open parses are canceled with it.
			const next = s.aclRevision + 1;
			await idb.insertAcl(c, sourceId, next, [], cmd.actorId, cmd.commandId);
			await idb.setAclRevision(c, sourceId, next);
			await idb.markDeleted(c, sourceId);
			for (const taskId of await idb.openIngestTasks(c, sourceId)) await this.tasks.cancelIn(c, taskId);
			// P16: open index work stops with readability; the vector points are
			// purged from every generation afterwards (Indexer.reconcile).
			for (const taskId of await idx.openIndexTasks(c, sourceId)) await this.tasks.cancelIn(c, taskId);
			await idx.staleEntries(c, sourceId);
			await idb.insertCommand(c, cmd.tenantId, cmd.commandId, {
				commandKind: "delete",
				requestDigest: cmd.requestDigest,
				sourceId,
			});
			// P2.7: a deletion survives a restore (removal inventory).
			removal = await this.removalIn(c, cmd, sourceId, "delete", next, []);
		});
		if (removal) await this.recordRemoval(removal);
		const source = await idb.getSource(this.store.pool, sourceId);
		if (!source) throw new SourceError("NOT_FOUND", "source");
		this.log.info("source deleted", { sourceId });
		return { source, existing: false };
	}
}

function isUniqueViolation(err: unknown): boolean {
	return typeof err === "object" && err !== null && (err as { code?: string }).code === "23505";
}
