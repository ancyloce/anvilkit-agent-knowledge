// The jobs contract as Knowledge consumes it (contracts/jobs, the verbatim
// copies in @anvilkit/generated-clients): the reviewed parser profiles and
// the schema every envelope Knowledge writes and every parser result it
// reads must satisfy. Nothing here trusts the parser: results are parsed
// strictly (duplicate keys refused) and validated before any field is read.
import schema from "@anvilkit/generated-clients/jobs/job.schema.json" with { type: "json" };
import profiles from "@anvilkit/generated-clients/jobs/profiles.json" with { type: "json" };
import { parseStrictJson } from "@anvilkit/generated-clients/validation/json";
import { Ajv2020, type ValidateFunction } from "ajv/dist/2020.js";

const ajv = new Ajv2020({ strict: true, strictRequired: false, allErrors: false });
ajv.addSchema(schema as object);
const compiled = new Map<string, ValidateFunction>();

function validator(ref: string): ValidateFunction {
	let v = compiled.get(ref);
	if (!v) {
		v = ajv.getSchema(`urn:anvilkit:jobs:v1${ref}`);
		if (!v) throw new Error(`jobs schema lacks ${ref}`);
		compiled.set(ref, v);
	}
	return v;
}

export class ContractViolation extends Error {}

export function validate(ref: string, value: unknown): void {
	const v = validator(ref);
	if (!v(value)) {
		const e = v.errors?.[0];
		throw new ContractViolation(`${ref}${e?.instancePath ?? ""}: ${e?.message ?? "invalid"}`);
	}
}

export interface ParserLimits {
	parserName: "docling";
	parserVersion: string;
	mediaTypes: string[];
	maxInputBytes: number;
	maxPages: number;
	maxDecompressedBytes: number;
	maxDecompressionRatio: number;
	maxArchiveEntries: number;
	maxOutputBytes: number;
	maxChunks: number;
	chunker: { chunkerId: string; revision: string; maxChunkChars: number };
}

export interface ParserProfile {
	profileId: string;
	revision: string;
	jobKind: "parser";
	image: { repository: string; digest: string };
	entrypoint: string[];
	resources: { cpu: string; memory: string };
	deadlineSeconds: number;
	runtimeClass?: "gvisor";
	candidateCode: false;
	parser: ParserLimits;
}

/** A reviewed parser profile by id, validated against the schema; any other kind is refused. */
export function parserProfile(id: string): ParserProfile {
	const p = (profiles as { profiles: { profileId: string; jobKind: string }[] }).profiles.find(
		(x) => x.profileId === id,
	);
	if (!p) throw new ContractViolation(`no reviewed profile ${id}`);
	validate("#/$defs/profile", p);
	if (p.jobKind !== "parser") throw new ContractViolation(`${id} is not a parser profile`);
	return p as unknown as ParserProfile;
}

/** Strict JSON: duplicate keys and non-finite numbers are refused before validation. */
const utf8 = new TextDecoder("utf-8", { fatal: true });

export function strictJson(bytes: Buffer): unknown {
	let text: string;
	try {
		text = utf8.decode(bytes);
	} catch {
		throw new ContractViolation("result is not UTF-8");
	}
	let value: unknown;
	try {
		// The shared strict boundary: every duplicate key is refused, even
		// one repeating the same value (lossless-json alone merges those).
		value = parseStrictJson(text);
	} catch (err) {
		throw new ContractViolation(`result is not strict JSON: ${err instanceof Error ? err.message : String(err)}`);
	}
	return value;
}
