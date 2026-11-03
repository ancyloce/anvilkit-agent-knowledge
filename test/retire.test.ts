// The retirement of a generation: one deadline bounds every pool drain
// (primary, cleanup, Store), the report names what the deadline cut short,
// and a drain failing before or after the deadline is logged, never thrown.
import type pg from "pg";
import { describe, expect, it } from "vitest";
import type { MemoryStorePort } from "../src/adapters/memorystore.js";
import type { ObjectStore } from "../src/adapters/objects.js";
import { type Runtime, retire } from "../src/generations.js";

type Drain = () => Promise<void>;

function runtime(drains: { database: Drain; cleanup?: Drain; store?: Drain }, closed: string[]): Runtime {
	return {
		gen: {} as Runtime["gen"],
		pool: { end: drains.database } as unknown as pg.Pool,
		cleanup: drains.cleanup ? ({ end: drains.cleanup } as unknown as pg.Pool) : undefined,
		memoryStore: drains.store ? ({ close: drains.store } as unknown as MemoryStorePort) : undefined,
		objects: { close: () => closed.push("objects") } as unknown as ObjectStore,
	};
}

function recorder() {
	const lines: { msg: string; fields?: Record<string, unknown> }[] = [];
	return { lines, warn: (msg: string, fields?: Record<string, unknown>) => lines.push({ msg, fields }) };
}

const never: Drain = () => new Promise<void>(() => {});
const quick: Drain = () => Promise.resolve();

describe("retire", () => {
	it("drains every pool and closes the clients after them", async () => {
		const closed: string[] = [];
		const order: string[] = [];
		const log = recorder();
		const forced = await retire(
			runtime(
				{
					database: async () => void order.push("database"),
					cleanup: async () => void order.push("cleanup"),
					store: async () => void order.push("store"),
				},
				closed,
			),
			1000,
			log,
		);
		expect(forced).toBe(false);
		expect(order.sort()).toEqual(["cleanup", "database", "store"]);
		expect(closed).toEqual(["objects"]);
		expect(log.lines).toEqual([]);
	});

	it("bounds the cleanup and Store drains by the same deadline and names what it cut short", async () => {
		const log = recorder();
		const started = Date.now();
		const forced = await retire(runtime({ database: quick, cleanup: never, store: never }, []), 50, log);
		expect(forced).toBe(true);
		expect(Date.now() - started).toBeLessThan(1000);
		expect(log.lines).toEqual([
			{ msg: "generation drain cut short by the limit", fields: { resources: "cleanup,store", limitMs: 50 } },
		]);
	});

	it("logs a drain that fails, before or after the deadline, without throwing", async () => {
		const log = recorder();
		let failLate!: (err: Error) => void;
		const late: Drain = () =>
			new Promise<void>((_resolve, reject) => {
				failLate = reject;
			});
		const forced = await retire(
			runtime(
				{
					database: late,
					cleanup: () => Promise.reject(new Error("Called end on pool more than once")),
					store: quick,
				},
				[],
			),
			50,
			log,
		);
		expect(forced).toBe(true);
		failLate(new Error("connection terminated"));
		await new Promise((r) => setTimeout(r, 10));
		expect(log.lines.map((l) => [l.msg, l.fields?.resource ?? l.fields?.resources])).toEqual([
			["generation drain failed", "cleanup"],
			["generation drain cut short by the limit", "database"],
			["generation drain failed", "database"],
		]);
	});
});
