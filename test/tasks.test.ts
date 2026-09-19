import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Store } from "../src/adapters/postgres.js";
import type { Tasks } from "../src/application/tasks.js";
import { digestOf, expectedResult, TaskError } from "../src/domain/task.js";
import {
	bounds,
	FakeClock,
	FakeDispatch,
	type Instance,
	localCheckInput,
	newTasks,
	sourceFixture,
	startInstance,
} from "./harness.js";

let inst: Instance;
let tasks: Tasks;
let store: Store;
let clock: FakeClock;
let dispatch: FakeDispatch;

beforeAll(async () => {
	inst = await startInstance();
	clock = new FakeClock(new Date("2026-09-17T12:00:00Z"));
	dispatch = new FakeDispatch();
	({ tasks, store } = newTasks(inst, clock, dispatch));
	await sourceFixture(inst, "tenant_a", "src_1");
});

afterAll(async () => {
	await store.pool.end();
	await inst.stop();
});

interface OutboxRow {
	uuid: string;
	destination_topic: string;
	metadata: Record<string, string>;
	envelope: Record<string, unknown>;
}

async function outboxRows(): Promise<OutboxRow[]> {
	const r = await store.pool.query<{ uuid: string; payload: string; metadata: string }>(
		`SELECT uuid, payload::text AS payload, metadata::text AS metadata FROM outbox ORDER BY transaction_id, "offset"`,
	);
	return r.rows.map((row) => {
		const wrapped = JSON.parse(row.payload) as {
			destination_topic: string;
			uuid: string;
			payload: string;
			metadata: Record<string, string>;
		};
		return {
			uuid: row.uuid,
			destination_topic: wrapped.destination_topic,
			metadata: wrapped.metadata,
			envelope: JSON.parse(Buffer.from(wrapped.payload, "base64").toString("utf8")) as Record<string, unknown>,
		};
	});
}

const request = (taskId: string, payload: string, extra: Record<string, unknown> = {}) =>
	tasks.request({
		taskId,
		tenantId: "tenant_a",
		kind: "local-check",
		profile: "local-check-v1",
		input: localCheckInput(payload),
		effects: "reconstructible",
		dispatchId: "",
		authorizationRef: "source:src_1",
		correlationId: `req_${taskId}`,
		...extra,
	});

describe("request and outbox", () => {
	it("commits the request with its event or nothing at all, and supersedes on new input", async () => {
		const { task, existing } = await request("task_1", "hello");
		expect(existing).toBe(false);
		expect(task.generation).toBe(1);
		expect(digestOf(task.input)).toBe(task.inputDigest);
		let rows = await outboxRows();
		expect(rows).toHaveLength(1);
		const row = rows[0] as OutboxRow;
		expect(row.uuid).toMatch(/^[0-9a-f-]{36}$/);
		expect(row.destination_topic).toBe("anvilkit.knowledge.background.requested");
		expect(row.metadata.anvilkit_event_type).toBe("background.requested");
		expect(row.envelope.eventType).toBe("background.requested");
		expect(row.envelope.producer).toBe("anvilkit-agent-knowledge");
		expect(row.envelope.aggregateRevision).toBe("1");
		expect((row.envelope.payload as Record<string, string>).taskKind).toBe("local-check");
		// Same input: existing, no event.
		const again = await request("task_1", "hello");
		expect(again.existing).toBe(true);
		expect((await outboxRows()).length).toBe(1);
		// A refusal inside the transaction leaves neither row nor event.
		await expect(request("task_2", "x", { profile: "parser-v1" })).rejects.toMatchObject({
			code: "PROFILE_UNQUALIFIED",
		});
		await expect(tasks.get("task_2")).rejects.toMatchObject({ code: "NOT_FOUND" });
		await expect(request("task_3", "x", { authorizationRef: "source:missing" })).rejects.toMatchObject({
			code: "FORBIDDEN",
		});
		await expect(request("task_4", "x", { tenantId: "tenant_b" })).rejects.toMatchObject({ code: "FORBIDDEN" });
		expect((await outboxRows()).length).toBe(1);
		// New input supersedes.
		const next = await request("task_1", "hello-2");
		expect(next.task.generation).toBe(2);
		rows = await outboxRows();
		expect(rows).toHaveLength(3);
		expect(rows[1]?.destination_topic).toBe("anvilkit.knowledge.background.completed");
		expect(((rows[1] as OutboxRow).envelope.payload as Record<string, string>).state).toBe("stale");
		const old = await store.pool.query(
			"SELECT state FROM background_requests WHERE task_id = 'task_1' AND generation = 1",
		);
		expect(old.rows[0]?.state).toBe("stale");
	});
});

describe("claims, fences and acceptance", () => {
	it("admits one racing claimant and fences the superseded one", async () => {
		const { task } = await request("task_r", "race");
		const results = await Promise.allSettled([
			tasks.claim(task.taskId, 1, "w0", 60_000),
			tasks.claim(task.taskId, 1, "w1", 60_000),
		]);
		const winners = results.filter((r) => r.status === "fulfilled");
		expect(winners).toHaveLength(1);
		const loser = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
		expect(loser.reason).toBeInstanceOf(TaskError);
		expect(["ALREADY_CLAIMED"]).toContain((loser.reason as TaskError).code);
		const winner = (await tasks.get(task.taskId)).workerId;
		await tasks.heartbeat(task.taskId, 1, winner);
		await expect(tasks.heartbeat(task.taskId, 1, "stranger")).rejects.toMatchObject({ code: "STALE_EXECUTION" });
		// Expiry: the sweeper reschedules; identity reuse is refused; a fresh identity takes over.
		clock.advance(120_000);
		expect(await tasks.sweepExpired(10)).toBe(1);
		expect((await tasks.get(task.taskId)).state).toBe("retry_scheduled");
		await expect(tasks.claim(task.taskId, 1, "w8", 60_000)).rejects.toMatchObject({ code: "NOT_CLAIMABLE" });
		clock.advance(bounds.retryDelayMs);
		await expect(tasks.claim(task.taskId, 1, winner, 60_000)).rejects.toMatchObject({ code: "WORKER_IDENTITY_REUSED" });
		const second = await tasks.claim(task.taskId, 1, "w9", 60_000);
		expect(digestOf(second.input)).toBe(task.inputDigest);
		const expected = expectedResult(task);
		await expect(
			tasks.submit(task.taskId, 1, {
				workerId: winner,
				inputDigest: task.inputDigest,
				succeeded: true,
				resultRef: expected.ref,
				resultDigest: expected.digest,
				failureCode: "",
			}),
		).rejects.toMatchObject({ code: "STALE_EXECUTION" });
		const d = await tasks.submit(task.taskId, 1, {
			workerId: "w9",
			inputDigest: task.inputDigest,
			succeeded: true,
			resultRef: expected.ref,
			resultDigest: expected.digest,
			failureCode: "",
		});
		expect(d.accepted).toBe(true);
		const rows = await outboxRows();
		const last = rows[rows.length - 1] as OutboxRow;
		expect(last.envelope.eventType).toBe("background.completed");
		expect((last.envelope.payload as Record<string, string>).resultDigest).toBe(expected.digest);
		// Repeated identical submission: existing; a different one: refused.
		const again = await tasks.submit(task.taskId, 1, {
			workerId: "w9",
			inputDigest: task.inputDigest,
			succeeded: true,
			resultRef: expected.ref,
			resultDigest: expected.digest,
			failureCode: "",
		});
		expect(again.existing).toBe(true);
		expect(again.accepted).toBe(true);
	});

	it("refuses digest mismatches, consumes profile mismatches and failures, cancels on revocation", async () => {
		const { task } = await request("task_s", "submit");
		await tasks.claim(task.taskId, 1, "w1", 60_000);
		const expected = expectedResult(task);
		const wrong = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
		await expect(
			tasks.submit(task.taskId, 1, {
				workerId: "w1",
				inputDigest: wrong,
				succeeded: true,
				resultRef: expected.ref,
				resultDigest: expected.digest,
				failureCode: "",
			}),
		).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
		expect((await tasks.get(task.taskId)).state).toBe("leased");
		const pm = await tasks.submit(task.taskId, 1, {
			workerId: "w1",
			inputDigest: task.inputDigest,
			succeeded: true,
			resultRef: expected.ref,
			resultDigest: wrong,
			failureCode: "",
		});
		expect(pm.accepted).toBe(false);
		expect(pm.task.state).toBe("retry_scheduled");
		expect(pm.task.failureCode).toBe("PROFILE_MISMATCH");
		clock.advance(bounds.retryDelayMs);
		await tasks.claim(task.taskId, 1, "w2", 60_000);
		await inst.admin("UPDATE sources SET deleted = true WHERE source_id = 'src_1'");
		const rv = await tasks.submit(task.taskId, 1, {
			workerId: "w2",
			inputDigest: task.inputDigest,
			succeeded: true,
			resultRef: expected.ref,
			resultDigest: expected.digest,
			failureCode: "",
		});
		expect(rv.accepted).toBe(false);
		expect(rv.task.state).toBe("canceled");
		expect(rv.task.failureCode).toBe("AUTHORIZATION_REVOKED");
		await inst.admin("UPDATE sources SET deleted = false WHERE source_id = 'src_1'");
		// A reported failure on the last attempt is dead.
		const t3 = (await request("task_s3", "submit-3")).task;
		for (const w of ["w1", "w2"]) {
			await tasks.claim(t3.taskId, 1, w, 60_000);
			const d = await tasks.submit(t3.taskId, 1, {
				workerId: w,
				inputDigest: t3.inputDigest,
				succeeded: false,
				resultRef: "",
				resultDigest: "",
				failureCode: "HANDLER_FAILED",
			});
			clock.advance(bounds.retryDelayMs);
			if (w === "w2") expect(d.task.state).toBe("dead");
		}
		const attempts = await store.pool.query(
			"SELECT outcome FROM task_attempts WHERE task_id = 'task_s3' ORDER BY ordinal",
		);
		expect(attempts.rows.map((r) => r.outcome)).toEqual(["failed", "failed"]);
		const cancel = await tasks.cancel("task_s3");
		expect(cancel.changed).toBe(false);
	});

	it("never blindly reassigns an external-effect lease", async () => {
		const { task } = await request("task_x", "ext", { effects: "external", dispatchId: "disp_1" });
		await tasks.claim(task.taskId, 1, "w1", 60_000);
		clock.advance(120_000);
		await expect(tasks.claim(task.taskId, 1, "w2", 60_000)).rejects.toMatchObject({ code: "EFFECT_UNCERTAIN" });
		dispatch.answer = "unknown";
		await tasks.sweepExpired(10);
		expect(dispatch.calls).toBe(1);
		const dead = await tasks.get(task.taskId);
		expect(dead.state).toBe("dead");
		expect(dead.failureCode).toBe("EFFECT_UNCERTAIN");
		const t2 = (await request("task_y", "ext2", { effects: "external", dispatchId: "disp_2" })).task;
		await tasks.claim(t2.taskId, 1, "w1", 60_000);
		clock.advance(120_000);
		dispatch.answer = "not_sent";
		await tasks.sweepExpired(10);
		expect((await tasks.get(t2.taskId)).state).toBe("retry_scheduled");
	});

	it("cancels a leased request and fences its claimant", async () => {
		const { task } = await request("task_c", "cancel");
		await tasks.claim(task.taskId, 1, "w1", 60_000);
		const c = await tasks.cancel(task.taskId);
		expect(c.changed).toBe(true);
		expect(c.task.state).toBe("canceled");
		await expect(tasks.heartbeat(task.taskId, 1, "w1")).rejects.toMatchObject({ code: "STALE_EXECUTION" });
		const rows = await outboxRows();
		expect(((rows[rows.length - 1] as OutboxRow).envelope.payload as Record<string, string>).state).toBe("canceled");
	});
});

it("F08 rejects new work after snapshot expiry while an in-flight task retains frozen bounds", async () => {
	const expiry = clock.now().getTime() + 1000;
	tasks.setExpiry(expiry);
	const { task } = await request("task_frozen_expiry", "frozen");
	await tasks.claim(task.taskId, 1, "frozen-worker", 60000);
	tasks.setBounds({ ...bounds, maxAttempts: 1, maxLeaseMs: 1000 });
	clock.advance(1001);
	expect(tasks.ready()).toBe(false);
	await expect(request("new_expired", "new")).rejects.toMatchObject({ code: "STALE_EXECUTION" });
	const heartbeat = await tasks.heartbeat(task.taskId, 1, "frozen-worker");
	expect(heartbeat.maxAttempts).toBe(bounds.maxAttempts);
	const expected = expectedResult(task);
	const done = await tasks.submit(task.taskId, 1, {
		workerId: "frozen-worker",
		inputDigest: task.inputDigest,
		succeeded: true,
		resultRef: expected.ref,
		resultDigest: expected.digest,
		failureCode: "",
	});
	expect(done.accepted).toBe(true);
	tasks.setExpiry(undefined);
	tasks.setBounds(bounds);
});
