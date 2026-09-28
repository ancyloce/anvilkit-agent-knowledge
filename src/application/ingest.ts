// The parse step of knowledge-ingest tasks (DD-07 §2): the Knowledge parser
// launcher behind IngestService.AdvanceParse and the owner's result records
// for the knowledge-ingest-v1 profile.
//
// AdvanceParse serves only the current claimant of the task's current
// attempt. Its first call commits the launch record (the create marker)
// under the task's row lock and before any Kubernetes request; the Job is
// named by the attempt's launch key, so a lost create answer is resolved by
// reading that name and a repeated create can only meet it. The stage
// Secret (one presigned GET for the revision's content-addressed object,
// one presigned PUT for the result key) is owned by the Job. When the Job
// completes, Knowledge reads the result bytes itself, validates them
// against the contract and the envelope and records their digest; the
// Pod's own report is never evidence. Acceptance re-reads the same bytes,
// re-checks their digest and commits the chunks, the ingest state and the
// task's acceptance in one transaction. No storage or Kubernetes I/O runs
// while a row is locked.
import * as idb from "../adapters/ingestdb.js";
import { ContractViolation, type ParserProfile, strictJson, validate } from "../adapters/jobcontract.js";
import { type Kube, KubeError } from "../adapters/kube.js";
import { ObjectMissing, type ObjectStore, ObjectTooLarge } from "../adapters/objects.js";
import * as db from "../adapters/postgres.js";
import {
	checkResult,
	chunkIdOf,
	envelopeOf,
	jobManifest,
	jobPhase,
	launchKeyOf,
	ParseError,
	type ParseResult,
	parseFailure,
	resultKeyOf,
	resultRefOf,
	type Template,
} from "../domain/parse.js";
import { type IngestInput, parseIngestInput } from "../domain/source.js";
import { digestOf, type Submission, type Task } from "../domain/task.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import type { IngestPlan } from "./sources.js";
import type { Clock, PreparedResult, ResultRecords } from "./tasks.js";

export type ParseAnswer =
	| { state: "launched" | "running"; retryAfterMs: number }
	| { state: "completed"; resultRef: string; resultDigest: string }
	| { state: "failed"; failureCode: string };

export interface LauncherSettings extends Template {
	pollMs: number;
	deadlineGraceMs: number;
	presignTtlSeconds: number;
}

interface Verified {
	input: IngestInput;
	launch: idb.ParseLaunch;
	result: ParseResult;
}

export class Ingest implements ResultRecords {
	constructor(
		private readonly store: db.Store,
		private readonly kube: () => Kube | undefined,
		private readonly objects: () => ObjectStore | undefined,
		private readonly profile: () => ParserProfile | undefined,
		private readonly settings: () => LauncherSettings,
		private readonly clock: Clock,
		private readonly log: Logger,
		private readonly metrics: Metrics,
	) {}

	private jobPath(key = ""): string {
		return `/apis/batch/v1/namespaces/${this.settings().namespace}/jobs${key ? `/${key}` : ""}`;
	}

	private secretPath(key = ""): string {
		return `/api/v1/namespaces/${this.settings().namespace}/secrets${key ? `/${key}` : ""}`;
	}

	/** The claimant's current task, checked against the request; nothing is written here. */
	private async claimed(taskId: string, generation: number, workerId: string, inputDigest: string): Promise<Task> {
		const task = await db.getRequest(this.store.pool, taskId, generation);
		if (!task) throw new ParseError("NOT_FOUND", `${taskId} generation ${generation}`);
		if (task.kind !== "knowledge-ingest") throw new ParseError("INVALID_ARGUMENT", "not a knowledge-ingest task");
		if (task.inputDigest !== inputDigest)
			throw new ParseError("INVALID_ARGUMENT", "input digest differs from the frozen input");
		const now = this.clock.now().getTime();
		if (task.state !== "leased" || task.workerId !== workerId || !task.leaseUntil || task.leaseUntil.getTime() <= now)
			throw new ParseError("STALE_EXECUTION", `not the current claimant (state ${task.state})`);
		return task;
	}

	async advance(taskId: string, generation: number, workerId: string, inputDigest: string): Promise<ParseAnswer> {
		const task = await this.claimed(taskId, generation, workerId, inputDigest);
		const input = parseIngestInput(task.input);
		const profile = this.profile();
		const kube = this.kube();
		const objects = this.objects();
		if (!profile || !kube || !objects) return { state: "failed", failureCode: "PARSER_UNAVAILABLE" };
		if (input.parserProfile !== profile.profileId || String(input.parserProfileRevision) !== profile.revision)
			return { state: "failed", failureCode: "PROFILE_UNQUALIFIED" };
		let launch = await idb.getLaunch(this.store.pool, taskId, generation, task.attemptCount);
		if (!launch) {
			const created = await this.recordLaunch(task, input, profile);
			if ("refused" in created) return { state: "failed", failureCode: created.refused };
			launch = created;
			await this.retireEarlier(launch);
		}
		if (launch.workerId !== workerId) throw new ParseError("STALE_EXECUTION", "another claimant's launch");
		if (launch.state === "completed")
			return { state: "completed", resultRef: resultRefOf(launch.launchKey), resultDigest: launch.resultDigest };
		if (launch.state === "failed") return { state: "failed", failureCode: launch.failureCode };
		return this.drive(launch, input, profile, kube, objects);
	}

	/** The create marker: committed with the claimant rechecked under the row lock, before any request. */
	private async recordLaunch(
		task: Task,
		input: IngestInput,
		profile: ParserProfile,
	): Promise<idb.ParseLaunch | { refused: string }> {
		return this.store.inTx(async (c) => {
			const locked = await db.getRequestForUpdate(c, task.taskId, task.generation);
			const now = this.clock.now();
			if (
				locked?.state !== "leased" ||
				locked.workerId !== task.workerId ||
				locked.attemptCount !== task.attemptCount ||
				!locked.leaseUntil ||
				locked.leaseUntil <= now
			)
				throw new ParseError("STALE_EXECUTION", "the claim changed");
			const existing = await idb.getLaunch(c, task.taskId, task.generation, task.attemptCount, true);
			if (existing) return existing;
			if ((await db.sourceAuthorization(c, locked.authorizationRef, locked.tenantId)) !== "current")
				return { refused: parseFailure.authorizationRevoked };
			const launch: idb.ParseLaunch = {
				taskId: task.taskId,
				generation: task.generation,
				attempt: task.attemptCount,
				launchKey: launchKeyOf(task.taskId, task.generation, task.attemptCount),
				workerId: task.workerId,
				profileId: profile.profileId,
				profileRevision: Number(profile.revision),
				inputDigest: input.input.digest,
				deadline: new Date(now.getTime() + profile.deadlineSeconds * 1000),
				state: "creating",
				jobUid: "",
				resultKey: "",
				resultDigest: "",
				resultSize: null,
				verdict: "",
				failureCode: "",
			};
			await idb.insertLaunch(c, launch);
			const ingest = await idb.getIngestByTask(c, task.taskId, true);
			if (ingest && ingest.state === "pending") await idb.updateIngest(c, ingest.requestId, { state: "parsing" });
			return launch;
		});
	}

	/** Earlier attempts' Jobs are superseded by this claim: deleted by their UID, best effort. */
	private async retireEarlier(launch: idb.ParseLaunch): Promise<void> {
		const kube = this.kube();
		if (!kube) return;
		for (const old of await idb.earlierLaunches(this.store.pool, launch.taskId, launch.generation, launch.attempt)) {
			try {
				await kube.delete(this.jobPath(old.launchKey), old.jobUid);
			} catch (err) {
				this.log.warn("superseded parser Job not deleted", { launchKey: old.launchKey, error: String(err) });
			}
		}
	}

	private async fail(launch: idb.ParseLaunch, code: string, outcome: string): Promise<ParseAnswer> {
		await idb.updateLaunch(this.store.pool, launch, { state: "failed", failureCode: code });
		this.metrics.parseResults.inc({ outcome });
		await this.cleanup(launch);
		return { state: "failed", failureCode: code };
	}

	private async cleanup(launch: idb.ParseLaunch): Promise<void> {
		const uid = launch.jobUid || (await this.observedUid(launch.launchKey));
		if (!uid) return;
		try {
			await this.kube()?.delete(this.jobPath(launch.launchKey), uid);
		} catch (err) {
			this.log.warn("parser Job not deleted", { launchKey: launch.launchKey, error: String(err) });
		}
	}

	private async observedUid(launchKey: string): Promise<string> {
		const job = await this.kube()
			?.get(this.jobPath(launchKey))
			.catch(() => undefined);
		return ((job?.metadata as { uid?: string } | undefined)?.uid ?? "") as string;
	}

	private async drive(
		launch: idb.ParseLaunch,
		input: IngestInput,
		profile: ParserProfile,
		kube: Kube,
		objects: ObjectStore,
	): Promise<ParseAnswer> {
		const s = this.settings();
		const now = this.clock.now().getTime();
		const env = envelopeOf(
			launch.launchKey,
			{ taskId: launch.taskId, generation: launch.generation, attempt: launch.attempt },
			profile,
			input.input,
			launch.deadline,
		);
		let job = await kube.get(this.jobPath(launch.launchKey));
		if (!job) {
			if (launch.jobUid) return this.fail(launch, parseFailure.resultMissing, "missing");
			if (now > launch.deadline.getTime()) return this.fail(launch, parseFailure.deadline, "deadline");
			validate("#/$defs/parseEnvelope", env);
			const annotations = {
				"anvilkit.io/task-id": launch.taskId,
				"anvilkit.io/generation": String(launch.generation),
				"anvilkit.io/attempt": String(launch.attempt),
			};
			const active = Math.max(1, Math.floor((launch.deadline.getTime() - now) / 1000));
			const manifest = jobManifest(
				{ ...s, stageSecret: `${launch.launchKey}-stage` },
				profile,
				env,
				annotations,
				active,
			);
			try {
				job = await kube.create(this.jobPath(), manifest);
				this.metrics.parseLaunches.inc({ outcome: "created" });
			} catch (err) {
				if (err instanceof KubeError && err.status === 409) {
					this.metrics.parseLaunches.inc({ outcome: "exists" });
					job = await kube.get(this.jobPath(launch.launchKey));
				} else if (err instanceof KubeError && err.status >= 400 && err.status < 500) {
					// Refused by the API server or admission: this attempt cannot run.
					this.metrics.parseLaunches.inc({ outcome: "refused" });
					this.log.warn("parser Job refused", {
						launchKey: launch.launchKey,
						status: err.status,
						reason: err.reason,
						detail: err.message,
					});
					return this.fail(launch, parseFailure.jobFailed, "job_failed");
				} else {
					// No answer: the name is read again on the next call.
					this.metrics.parseLaunches.inc({ outcome: "unresolved" });
					this.log.warn("parser Job create unresolved", { launchKey: launch.launchKey, error: String(err) });
					return { state: "launched", retryAfterMs: s.pollMs };
				}
			}
			if (!job) return { state: "launched", retryAfterMs: s.pollMs };
		}
		const meta = (job.metadata ?? {}) as { uid?: string; annotations?: Record<string, string> };
		const a = meta.annotations ?? {};
		if (
			a["anvilkit.io/task-id"] !== launch.taskId ||
			a["anvilkit.io/generation"] !== String(launch.generation) ||
			a["anvilkit.io/attempt"] !== String(launch.attempt) ||
			!meta.uid
		)
			return this.fail(launch, parseFailure.jobFailed, "job_failed");
		if (launch.jobUid !== meta.uid) {
			await idb.updateLaunch(this.store.pool, launch, { state: "running", jobUid: meta.uid });
			launch = { ...launch, state: "running", jobUid: meta.uid };
		}
		await this.ensureStageSecret(launch, input, kube, objects, meta.uid);
		switch (jobPhase(job)) {
			case "running":
				if (now > launch.deadline.getTime() + s.deadlineGraceMs)
					return this.fail(launch, parseFailure.deadline, "deadline");
				return { state: "running", retryAfterMs: s.pollMs };
			case "failed": {
				const conds = ((job.status as { conditions?: { type: string; reason?: string }[] })?.conditions ?? []).find(
					(c) => c.type === "Failed",
				);
				return conds?.reason === "DeadlineExceeded"
					? this.fail(launch, parseFailure.deadline, "deadline")
					: this.fail(launch, parseFailure.jobFailed, "job_failed");
			}
			case "succeeded":
				return this.accept(launch, env, profile, objects);
			default:
				return { state: "launched", retryAfterMs: s.pollMs };
		}
	}

	/** The Job-owned Secret with the attempt's two presigned URLs; created once, never logged. */
	private async ensureStageSecret(
		launch: idb.ParseLaunch,
		input: IngestInput,
		kube: Kube,
		objects: ObjectStore,
		jobUid: string,
	): Promise<void> {
		const name = `${launch.launchKey}-stage`;
		if (await kube.get(this.secretPath(name))) return;
		const objectKey = await idb.revisionObject(
			this.store.pool,
			input.sourceId,
			input.sourceRevision,
			input.input.digest,
		);
		if (!objectKey) throw new ParseError("NOT_FOUND", "source revision object");
		const ttl = Math.min(
			this.settings().presignTtlSeconds,
			Math.max(60, Math.ceil((launch.deadline.getTime() - this.clock.now().getTime()) / 1000) + 60),
		);
		const body = {
			apiVersion: "v1",
			kind: "Secret",
			metadata: {
				name,
				namespace: this.settings().namespace,
				labels: {
					"app.kubernetes.io/managed-by": "anvilkit-agent-knowledge",
					"anvilkit.io/launch-key": launch.launchKey,
				},
				ownerReferences: [{ apiVersion: "batch/v1", kind: "Job", name: launch.launchKey, uid: jobUid }],
			},
			type: "Opaque",
			stringData: {
				get: await objects.presignGet(objectKey, ttl),
				put: await objects.presignPut(resultKeyOf(launch.launchKey), ttl),
			},
		};
		try {
			await kube.create(this.secretPath(), body);
		} catch (err) {
			if (!(err instanceof KubeError && err.status === 409)) throw err;
		}
	}

	/** Reads, validates and records the result bytes of a completed Job. */
	private async accept(
		launch: idb.ParseLaunch,
		env: ReturnType<typeof envelopeOf>,
		profile: ParserProfile,
		objects: ObjectStore,
	): Promise<ParseAnswer> {
		const key = resultKeyOf(launch.launchKey);
		let bytes: Buffer;
		try {
			bytes = await objects.read(key, profile.parser.maxOutputBytes);
		} catch (err) {
			if (err instanceof ObjectMissing) return this.fail(launch, parseFailure.resultMissing, "missing");
			if (err instanceof ObjectTooLarge) return this.fail(launch, parseFailure.resultInvalid, "invalid");
			throw err;
		}
		let result: ParseResult;
		try {
			result = this.verify(bytes, env, profile);
		} catch (err) {
			this.log.warn("parser result refused", { launchKey: launch.launchKey, reason: String(err).slice(0, 200) });
			return this.fail(launch, parseFailure.resultInvalid, "invalid");
		}
		const resultDigest = digestOf(bytes);
		await idb.updateLaunch(this.store.pool, launch, {
			state: "completed",
			resultKey: key,
			resultDigest,
			resultSize: bytes.length,
			verdict: result.verdict,
			failureCode: result.failureCode ?? "",
		});
		this.metrics.parseResults.inc({ outcome: result.verdict });
		await this.cleanup({ ...launch, jobUid: launch.jobUid });
		return { state: "completed", resultRef: resultRefOf(launch.launchKey), resultDigest };
	}

	private verify(bytes: Buffer, env: ReturnType<typeof envelopeOf>, profile: ParserProfile): ParseResult {
		const value = strictJson(bytes);
		validate("#/$defs/parseResult", value);
		const result = value as ParseResult;
		checkResult(result, env, profile, (t) => digestOf(Buffer.from(t, "utf8")));
		return result;
	}

	// ---------------------------------------------------------------------
	// ResultRecords of knowledge-ingest-v1
	// ---------------------------------------------------------------------

	async prepare(task: Task, sub: Submission): Promise<PreparedResult | undefined> {
		const profile = this.profile();
		const objects = this.objects();
		if (!profile || !objects) return undefined;
		const launch = await idb.getLaunch(this.store.pool, task.taskId, task.generation, task.attemptCount);
		if (launch?.state !== "completed" || launch.workerId !== sub.workerId) return undefined;
		const input = parseIngestInput(task.input);
		let bytes: Buffer;
		try {
			bytes = await objects.read(launch.resultKey, profile.parser.maxOutputBytes);
		} catch (err) {
			if (err instanceof ObjectMissing || err instanceof ObjectTooLarge) return undefined;
			throw err;
		}
		if (digestOf(bytes) !== launch.resultDigest) return undefined;
		const env = envelopeOf(
			launch.launchKey,
			{ taskId: task.taskId, generation: task.generation, attempt: launch.attempt },
			profile,
			input.input,
			launch.deadline,
		);
		let result: ParseResult;
		try {
			result = this.verify(bytes, env, profile);
		} catch (err) {
			if (err instanceof ContractViolation || err instanceof ParseError) return undefined;
			throw err;
		}
		const verified: Verified = { input, launch, result };
		return { expected: { ref: resultRefOf(launch.launchKey), digest: launch.resultDigest }, payload: verified };
	}

	async onAccepted(c: db.PoolClient, task: Task, prepared: PreparedResult): Promise<void> {
		const { input, launch, result } = prepared.payload as Verified;
		const ingest = await idb.getIngestByTask(c, task.taskId, true);
		if (!ingest) return;
		if (ingest.sourceId !== input.sourceId || ingest.sourceRevision !== input.sourceRevision)
			throw new ParseError("INVALID_ARGUMENT", "ingest request does not match the task input");
		const common = {
			resultRef: resultRefOf(launch.launchKey),
			resultDigest: launch.resultDigest,
		};
		if (result.verdict === "rejected") {
			await idb.updateIngest(c, ingest.requestId, {
				...common,
				state: "failed",
				failureCode: result.failureCode ?? "",
				pageCount: 0,
				chunkCount: 0,
			});
			return;
		}
		await idb.insertChunks(
			c,
			result.chunks.map((ch) => ({
				chunkId: chunkIdOf({
					sourceId: ingest.sourceId,
					sourceRevision: ingest.sourceRevision,
					parserProfile: ingest.parserProfile,
					parserProfileRevision: ingest.parserProfileRevision,
					chunkerProfile: ingest.chunkerProfile,
					chunkerRevision: ingest.chunkerRevision,
					ordinal: ch.ordinal,
				}),
				sourceId: ingest.sourceId,
				sourceRevision: ingest.sourceRevision,
				parserProfile: ingest.parserProfile,
				parserProfileRevision: ingest.parserProfileRevision,
				chunkerProfile: ingest.chunkerProfile,
				chunkerRevision: ingest.chunkerRevision,
				ordinal: ch.ordinal,
				locator: JSON.stringify(ch.locator),
				contentDigest: ch.contentDigest,
				contentRef: `${launch.resultKey}#${ch.ordinal}`,
				qualityFlags: ch.qualityFlags,
				ingestRequestId: ingest.requestId,
			})),
		);
		// Parsed and accepted; the index generation of P16 moves it to indexed.
		await idb.updateIngest(c, ingest.requestId, {
			...common,
			state: "indexing",
			pageCount: result.pageCount,
			chunkCount: result.chunks.length,
		});
	}

	async onEnded(c: db.PoolClient, task: Task): Promise<void> {
		const ingest = await idb.getIngestByTask(c, task.taskId, true);
		if (!ingest || (ingest.state !== "pending" && ingest.state !== "parsing")) return;
		await idb.updateIngest(c, ingest.requestId, {
			state: task.state === "stale" ? "stale" : "failed",
			failureCode: task.failureCode || task.state.toUpperCase(),
		});
	}
}

export { KubeError };

/** The plan a registration freezes from the reviewed parser profile. */
export function planOf(p: ParserProfile): IngestPlan {
	return {
		profileId: p.profileId,
		profileRevision: Number(p.revision),
		chunkerId: p.parser.chunker.chunkerId,
		chunkerRevision: Number(p.parser.chunker.revision),
		mediaTypes: p.parser.mediaTypes,
		maxInputBytes: p.parser.maxInputBytes,
	};
}
