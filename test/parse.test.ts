import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { admitParserProfile, parserProfile } from "../src/adapters/jobcontract.js";
import { envelopeOf, jobManifest, type Template } from "../src/domain/parse.js";

// P0.7 (PAR-01): the rendered parser Job, without a database or a cluster.
const profile = parserProfile("parser-docling-dev-v1");
const launchKey = "parse-0123456789abcdef0123456789abcdef";
const template: Template = {
	namespace: "anvilkit-parsing",
	imageRegistry: "localhost:5001",
	nodePool: "components",
	seccompProfile: "anvilkit/candidate.json",
	stageSecret: `${launchKey}-stage`,
};
const envelope = envelopeOf(
	launchKey,
	{ taskId: "task_0123456789abcdef", generation: 1, attempt: 1 },
	profile,
	{ digest: `sha256:${"0".repeat(64)}`, sizeBytes: "6", mediaType: "text/markdown" },
	new Date("2026-10-08T12:00:00Z"),
);
const job = jobManifest(template, profile, envelope, { "anvilkit.io/attempt": "1" }, 600) as {
	metadata: { name: string; namespace: string; labels: Record<string, string> };
	spec: { template: { metadata: { labels: Record<string, string> }; spec: Record<string, unknown> } };
};
type Mount = { name: string; mountPath: string; readOnly?: boolean };
type Container = {
	name: string;
	volumeMounts: Mount[];
	resources: { requests: Record<string, string>; limits: Record<string, string> };
};
const pod = job.spec.template.spec as {
	initContainers: Container[];
	containers: Container[];
	volumes: { name: string; emptyDir?: { sizeLimit?: string; medium?: string } }[];
};
const all = [...pod.initContainers, ...pod.containers];
const parse = all.find((c) => c.name === "parse") as Container;
const stagers = all.filter((c) => c.name !== "parse");

describe("parser Job template", () => {
	it("shares no writable volume between the parser and a stager", () => {
		for (const stager of stagers) {
			for (const m of stager.volumeMounts) {
				const p = parse.volumeMounts.find((x) => x.name === m.name);
				if (!p) continue;
				// Of the volumes both mount: the parser reads stage-in, the stager reads stage-out.
				const parserReadsInput = m.name === "stage-in" && p.readOnly === true;
				const stagerReadsResult = m.name === "stage-out" && m.readOnly === true;
				expect(parserReadsInput || stagerReadsResult, `${m.name} shared by parse and ${stager.name}`).toBe(true);
			}
		}
		expect(parse.volumeMounts.find((m) => m.mountPath === "/tmp")?.name).toBe("tmp-parse");
		for (const stager of stagers)
			expect(stager.volumeMounts.find((m) => m.mountPath === "/tmp")?.name).toBe("tmp-stage");
		expect(parse.volumeMounts.map((m) => m.name)).not.toContain("tmp-stage");
		expect(parse.volumeMounts.map((m) => m.name)).not.toContain("stage-urls");
	});

	it("bounds every emptyDir on disk and declares every container's resources", () => {
		for (const v of pod.volumes.filter((x) => x.emptyDir)) {
			expect(v.emptyDir?.sizeLimit, v.name).toMatch(/^\d+(Mi|Gi)$/);
			expect(v.emptyDir?.medium ?? "", v.name).toBe("");
		}
		for (const c of all) {
			for (const k of ["cpu", "memory"]) {
				expect(c.resources.requests[k], `${c.name} requests.${k}`).toBeTruthy();
				expect(c.resources.limits[k], `${c.name} limits.${k}`).toBeTruthy();
			}
		}
	});

	// The admission suite's fixtures (deploy/policies/tests/resources): the Job and
	// its Pod exactly as this template renders them (JSON is YAML).
	it.runIf(process.env.ANVILKIT_RENDER_POLICY_FIXTURES)("renders the policy fixtures", () => {
		const dir = process.env.ANVILKIT_RENDER_POLICY_FIXTURES as string;
		const podDoc = {
			apiVersion: "v1",
			kind: "Pod",
			metadata: {
				name: job.metadata.name,
				namespace: job.metadata.namespace,
				labels: job.spec.template.metadata.labels,
			},
			spec: job.spec.template.spec,
		};
		writeFileSync(join(dir, "parser-docling-dev-v1-job.yaml"), `${JSON.stringify(job, null, 2)}\n`);
		writeFileSync(join(dir, "parser-docling-dev-v1-pod.yaml"), `${JSON.stringify(podDoc, null, 2)}\n`);
	});
});

// P0.8: the profile's state decides before the environment's choice.
describe("parser profile state", () => {
	it("refuses a disabled profile everywhere and a development one outside the guard", () => {
		expect(() => admitParserProfile(parserProfile("parser-docling-v1"), true)).toThrow(
			/disabled until its qualification/,
		);
		expect(() => admitParserProfile(parserProfile("parser-docling-dev-v1"), false)).toThrow(/development.enabled/);
		expect(admitParserProfile(parserProfile("parser-docling-dev-v1"), true).profileId).toBe("parser-docling-dev-v1");
	});
});
