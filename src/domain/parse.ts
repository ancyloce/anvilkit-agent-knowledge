// The parse step's rules (DD-07 §2): the launch identity of a claimed
// attempt, the fixed parser Job template, the trusted acceptance of the
// parser's result and the chunk identities derived from an accepted one.
// Pure functions: the launcher performs the Kubernetes and storage I/O.
import { createHash } from "node:crypto";
import type { ParserProfile } from "../adapters/jobcontract.js";

export const parseFailure = {
	resultInvalid: "PARSE_RESULT_INVALID",
	resultMissing: "PARSE_RESULT_MISSING",
	jobFailed: "PARSER_JOB_FAILED",
	deadline: "DEADLINE_EXCEEDED",
	authorizationRevoked: "AUTHORIZATION_REVOKED",
	superseded: "SUPERSEDED",
} as const;

export class ParseError extends Error {
	constructor(
		readonly code: "STALE_EXECUTION" | "INVALID_ARGUMENT" | "NOT_FOUND" | "UNAVAILABLE",
		message: string,
	) {
		super(`${code}: ${message}`);
	}
}

function sha(...parts: (string | number)[]): string {
	const h = createHash("sha256");
	for (const p of parts) h.update(String(p)).update("\0");
	return h.digest("hex");
}

/** One Job name per claimed attempt: a repeated create can only meet the same name. */
export function launchKeyOf(taskId: string, generation: number, attempt: number): string {
	return `parse-${sha("parse", taskId, generation, attempt).slice(0, 40)}`;
}

export const resultKeyOf = (launchKey: string): string => `parse/${launchKey}/result.json`;
export const resultRefOf = (launchKey: string): string => `parse:${launchKey}`;

export function chunkIdOf(c: {
	sourceId: string;
	sourceRevision: number;
	parserProfile: string;
	parserProfileRevision: number;
	chunkerProfile: string;
	chunkerRevision: number;
	ordinal: number;
}): string {
	return `chk-${sha(
		"chunk",
		c.sourceId,
		c.sourceRevision,
		c.parserProfile,
		c.parserProfileRevision,
		c.chunkerProfile,
		c.chunkerRevision,
		c.ordinal,
	).slice(0, 40)}`;
}

export interface ParseEnvelope {
	schemaVersion: 1;
	launchId: string;
	launchKey: string;
	taskId: string;
	generation: string;
	attempt: string;
	profileId: string;
	profileRevision: string;
	jobKind: "parser";
	deadline: string;
	input: { digest: string; sizeBytes: string; mediaType: string };
}

export function envelopeOf(
	launchKey: string,
	task: { taskId: string; generation: number; attempt: number },
	profile: ParserProfile,
	input: ParseEnvelope["input"],
	deadline: Date,
): ParseEnvelope {
	return {
		schemaVersion: 1,
		launchId: launchKey,
		launchKey,
		taskId: task.taskId,
		generation: String(task.generation),
		attempt: String(task.attempt),
		profileId: profile.profileId,
		profileRevision: profile.revision,
		jobKind: "parser",
		deadline: `${deadline.toISOString().slice(0, 19)}Z`,
		input,
	};
}

export interface Template {
	namespace: string;
	imageRegistry: string;
	nodePool: string;
	seccompProfile: string;
	stageSecret: string;
}

const labels = (launchKey: string, profile: ParserProfile) => ({
	"app.kubernetes.io/managed-by": "anvilkit-agent-knowledge",
	"anvilkit.io/job-kind": "parser",
	"anvilkit.io/launch-key": launchKey,
	"anvilkit.io/profile-id": profile.profileId,
});

function imageRef(t: Template, img: { repository: string; digest: string }): string {
	const first = img.repository.split("/")[0] ?? "";
	const hasRegistry =
		img.repository.includes("/") && (first.includes(".") || first.includes(":") || first === "localhost");
	return `${hasRegistry || !t.imageRegistry ? img.repository : `${t.imageRegistry}/${img.repository}`}@${img.digest}`;
}

const restricted = (uid: number, readOnly = true) => ({
	runAsNonRoot: true,
	runAsUser: uid,
	runAsGroup: 10001,
	allowPrivilegeEscalation: false,
	readOnlyRootFilesystem: readOnly,
	capabilities: { drop: ["ALL"] },
});

/**
 * The fixed parser Job: one Pod, no retries, the absolute deadline, no
 * ServiceAccount token or service links, emptyDir volumes only. The
 * trusted stager (UID 10002) alone mounts the stage Secret with the two
 * presigned URLs; the parser (UID 10001) runs between the two stage steps
 * under the AF_UNIX-only syscall profile with the input read-only and no
 * credential.
 */
export function jobManifest(
	t: Template,
	profile: ParserProfile,
	env: ParseEnvelope,
	annotations: Record<string, string>,
	activeDeadlineSeconds: number,
): Record<string, unknown> {
	const image = imageRef(t, profile.image);
	const envVars = [
		{ name: "ANVILKIT_PARSE_ENVELOPE", value: JSON.stringify(env) },
		{ name: "ANVILKIT_PARSER_LIMITS", value: JSON.stringify(profile.parser) },
	];
	const mb = (n: number) => `${Math.ceil(n / (1 << 20)) + 1}Mi`;
	const stageResources = { requests: { cpu: "50m", memory: "64Mi" }, limits: { cpu: "500m", memory: "256Mi" } };
	const podLabels = labels(env.launchKey, profile);
	return {
		apiVersion: "batch/v1",
		kind: "Job",
		metadata: { name: env.launchKey, namespace: t.namespace, labels: podLabels, annotations },
		spec: {
			backoffLimit: 0,
			completions: 1,
			parallelism: 1,
			activeDeadlineSeconds,
			// A backstop only: the launcher deletes each Job after reading its
			// result and reaps the Jobs of ended attempts.
			ttlSecondsAfterFinished: 3600,
			template: {
				metadata: { labels: podLabels },
				spec: {
					restartPolicy: "Never",
					automountServiceAccountToken: false,
					enableServiceLinks: false,
					...(profile.runtimeClass ? { runtimeClassName: profile.runtimeClass } : {}),
					...(t.nodePool ? { nodeSelector: { "anvilkit.io/pool": t.nodePool } } : {}),
					securityContext: {
						runAsNonRoot: true,
						fsGroup: 10001,
						seccompProfile: { type: "RuntimeDefault" },
					},
					volumes: [
						{ name: "stage-in", emptyDir: { sizeLimit: mb(profile.parser.maxInputBytes) } },
						{ name: "stage-out", emptyDir: { sizeLimit: mb(profile.parser.maxOutputBytes) } },
						{ name: "tmp", emptyDir: { sizeLimit: "1Gi" } },
						{ name: "stage-urls", secret: { secretName: t.stageSecret, defaultMode: 0o400 } },
					],
					initContainers: [
						{
							name: "stage-in",
							image,
							command: ["/usr/local/bin/anvilkit-stage", "in"],
							env: envVars,
							securityContext: restricted(10002),
							resources: stageResources,
							volumeMounts: [
								{ name: "stage-in", mountPath: "/stage/in" },
								{ name: "stage-urls", mountPath: "/run/anvilkit/stage", readOnly: true },
								{ name: "tmp", mountPath: "/tmp" },
							],
						},
						{
							name: "parse",
							image,
							command: profile.entrypoint,
							env: envVars,
							securityContext: {
								...restricted(10001),
								...(t.seccompProfile
									? { seccompProfile: { type: "Localhost", localhostProfile: t.seccompProfile } }
									: {}),
							},
							resources: {
								requests: { cpu: profile.resources.cpu, memory: profile.resources.memory },
								limits: { cpu: profile.resources.cpu, memory: profile.resources.memory },
							},
							volumeMounts: [
								{ name: "stage-in", mountPath: "/stage/in", readOnly: true },
								{ name: "stage-out", mountPath: "/stage/out" },
								{ name: "tmp", mountPath: "/tmp" },
							],
						},
					],
					containers: [
						{
							name: "stage-out",
							image,
							command: ["/usr/local/bin/anvilkit-stage", "out"],
							env: envVars,
							securityContext: restricted(10002),
							resources: stageResources,
							terminationMessagePolicy: "File",
							volumeMounts: [
								{ name: "stage-out", mountPath: "/stage/out", readOnly: true },
								{ name: "stage-urls", mountPath: "/run/anvilkit/stage", readOnly: true },
								{ name: "tmp", mountPath: "/tmp" },
							],
						},
					],
				},
			},
		},
	};
}

function codePoints(s: string): number {
	let n = 0;
	for (const _ of s) n++;
	return n;
}

export type JobPhase = "running" | "succeeded" | "failed" | "missing";

/** The Job's terminal condition, if any; a missing Job is reported as such. */
export function jobPhase(job: Record<string, unknown> | undefined): JobPhase {
	if (!job) return "missing";
	const conds = ((job.status as { conditions?: { type: string; status: string }[] } | undefined)?.conditions ?? []) as {
		type: string;
		status: string;
	}[];
	if (conds.some((c) => c.type === "Complete" && c.status === "True")) return "succeeded";
	if (conds.some((c) => c.type === "Failed" && c.status === "True")) return "failed";
	return "running";
}

export interface ParseChunk {
	ordinal: number;
	text: string;
	contentDigest: string;
	locator: { element: string; headingPath: string[]; page?: number; lineStart?: number; lineEnd?: number };
	qualityFlags: string[];
}

export interface ParseResult {
	schemaVersion: 1;
	launchKey: string;
	profileId: string;
	profileRevision: string;
	verdict: "parsed" | "rejected";
	failureCode?: string;
	input: ParseEnvelope["input"];
	parser: { name: string; version: string };
	chunker: { chunkerId: string; revision: string };
	pageCount: number;
	chunks: ParseChunk[];
	completedAt: string;
}

/**
 * The trusted acceptance of a schema-valid result: it must answer this
 * launch, profile, input, parser and chunker exactly, stay within the
 * profile's bounds, number its chunks 0..n-1 and carry each chunk's true
 * content digest. Any deviation is refused; document content is never read
 * for authority.
 */
export function checkResult(
	r: ParseResult,
	env: ParseEnvelope,
	profile: ParserProfile,
	digestOf: (s: string) => string,
): void {
	const fail = (why: string) => {
		throw new ParseError("INVALID_ARGUMENT", `parse result: ${why}`);
	};
	if (r.launchKey !== env.launchKey) fail("another launch");
	if (r.profileId !== profile.profileId || r.profileRevision !== profile.revision) fail("another profile");
	if (
		r.input.digest !== env.input.digest ||
		r.input.sizeBytes !== env.input.sizeBytes ||
		r.input.mediaType !== env.input.mediaType
	)
		fail("another input");
	if (r.parser.name !== profile.parser.parserName || r.parser.version !== profile.parser.parserVersion)
		fail("another parser");
	if (
		r.chunker.chunkerId !== profile.parser.chunker.chunkerId ||
		r.chunker.revision !== profile.parser.chunker.revision
	)
		fail("another chunker");
	if (r.pageCount > profile.parser.maxPages) fail("page bound");
	if (r.chunks.length > profile.parser.maxChunks) fail("chunk bound");
	r.chunks.forEach((c, i) => {
		if (c.ordinal !== i) fail(`chunk ${i} ordinal`);
		// Characters are code points, as the parser and the schema count them
		// (String.length would count UTF-16 units and refuse valid emoji/CJK).
		if (codePoints(c.text) > profile.parser.chunker.maxChunkChars) fail(`chunk ${i} length`);
		if (c.contentDigest !== digestOf(c.text)) fail(`chunk ${i} digest`);
		if (c.locator.lineStart !== undefined && (c.locator.lineEnd ?? 0) < c.locator.lineStart)
			fail(`chunk ${i} line range`);
	});
}
