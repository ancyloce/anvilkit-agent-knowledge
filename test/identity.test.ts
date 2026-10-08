// P0.1 identity transport of Knowledge: handshake refusals (AC1), workload
// authorization ahead of the handler (AC2), rotation (AC4: leaf swaps,
// the transition bundle, retirement, invalid updates, long-lived
// connections, no session resumption) and the completeness check.
import { X509Certificate } from "node:crypto";
import { rmSync, symlinkSync } from "node:fs";
import { connect as http2Connect, type IncomingHttpHeaders } from "node:http2";
import path from "node:path";
import { type TLSSocket, connect as tlsConnect } from "node:tls";
import {
	type ChannelCredentials,
	type ChannelOptions,
	credentials,
	Metadata,
	makeGenericClientConstructor,
	Server,
	type ServiceError,
	status,
} from "@grpc/grpc-js";
import { service as HealthService } from "grpc-health-check";
import { describe, expect, it } from "vitest";
import {
	anyAuthenticated,
	clientCredentials,
	clientOptions,
	createAuthorizer,
	IdentityWatcher,
	parseMaterial,
	parseSpiffe,
	peerPrincipal,
	serverCredentials,
	type Workload,
} from "../src/transport/identity.js";
import { files, issue, mount, newCA, spiffe, tempDir } from "./pki.js";

const Health = makeGenericClientConstructor(HealthService, "Health");
type HealthClient = InstanceType<typeof Health>;
const health = (address: string, creds: ChannelCredentials, options?: ChannelOptions): HealthClient =>
	new Health(address, creds, options);

const td = "anvilkit.local";
const apiURI = spiffe(td, "anvilkit-apps", "anvilkit-agent-api");
const bwURI = spiffe(td, "anvilkit-apps", "anvilkit-agent-background-worker");
const knowledgeURI = spiffe(td, "anvilkit-apps", "anvilkit-agent-knowledge");
const api: Workload = { namespace: "anvilkit-apps", serviceAccount: "anvilkit-agent-api" };
const healthPolicy = {
	"/grpc.health.v1.Health/Check": [api],
	"/grpc.health.v1.Health/Watch": [api],
	"/grpc.health.v1.Health/List": [api],
};

type Unary = (
	req: unknown,
	md: Metadata,
	opts: { deadline: number },
	cb: (err: ServiceError | null, res: unknown) => void,
) => void;

function check(client: HealthClient): Promise<unknown> {
	const call = (client as unknown as { check: Unary }).check.bind(client);
	return new Promise((resolve, reject) =>
		call({}, new Metadata(), { deadline: Date.now() + 3000 }, (err, res) => (err ? reject(err) : resolve(res))),
	);
}

async function code(p: Promise<unknown>): Promise<number | "ok"> {
	try {
		await p;
		return "ok";
	} catch (err) {
		return (err as ServiceError).code;
	}
}

interface Fixture {
	port: number;
	watcher: IdentityWatcher;
	dir: string;
	handled: () => number;
	server: Server;
}

async function startServer(caPem: Buffer, leaf: ReturnType<typeof issue>, policy = healthPolicy): Promise<Fixture> {
	const dir = tempDir();
	mount(dir, leaf, caPem);
	const watcher = new IdentityWatcher(files(dir), 50);
	watcher.start();
	const authorizer = createAuthorizer({ trustDomain: td, policy, trustedCas: () => watcher.current().cas });
	let handled = 0;
	const server = new Server({ interceptors: [authorizer.interceptor] });
	server.addService(HealthService, {
		check: (_call: unknown, cb: (e: null, r: unknown) => void) => {
			handled++;
			cb(null, { status: 1 });
		},
		watch: () => {},
		list: (_call: unknown, cb: (e: null, r: unknown) => void) => cb(null, { statuses: {} }),
	});
	authorizer.check(Object.values(HealthService).map((m) => m.path));
	const port = await new Promise<number>((resolve, reject) =>
		server.bindAsync("127.0.0.1:0", serverCredentials(watcher), (err, p) => (err ? reject(err) : resolve(p))),
	);
	return { port, watcher, dir, handled: () => handled, server };
}

function client(port: number, dir: string): HealthClient {
	const w = new IdentityWatcher(files(dir), 50);
	return health(`127.0.0.1:${port}`, clientCredentials(w), clientOptions("anvilkit-agent-knowledge"));
}

function clientDir(ca: ReturnType<typeof newCA>, bundle: Buffer, cn: string, uris: string[]): string {
	const dir = tempDir();
	mount(dir, issue(ca, cn, uris), bundle);
	return dir;
}

const serverLeaf = (ca: ReturnType<typeof newCA>) =>
	issue(ca, "anvilkit-agent-knowledge", [knowledgeURI], ["anvilkit-agent-knowledge"]);

describe("workload identity", () => {
	it("parses the complete SPIFFE form strictly", () => {
		expect(parseSpiffe(apiURI)).toEqual({
			trustDomain: td,
			namespace: "anvilkit-apps",
			serviceAccount: "anvilkit-agent-api",
		});
		for (const bad of [
			"https://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-api",
			"spiffe://anvilkit.local/sa/anvilkit-agent-api",
			"spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-api/extra",
			"spiffe://anvilkit.local/ns/Anvilkit-Apps/sa/anvilkit-agent-api",
			"spiffe://anvilkit.local:443/ns/anvilkit-apps/sa/anvilkit-agent-api",
			"spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-api?x=1",
			"spiffe:///ns/anvilkit-apps/sa/anvilkit-agent-api",
		])
			expect(() => parseSpiffe(bad), bad).toThrow();
	});

	it("rejects defective material and certificates without exactly one URI SAN", () => {
		const ca = newCA("ca");
		const leaf = issue(ca, "x", [apiURI]);
		const other = issue(ca, "y", [apiURI]);
		expect(() => parseMaterial(leaf.certPem, other.keyPem, ca.pem)).toThrow(/key values mismatch/);
		expect(() => parseMaterial(leaf.certPem, leaf.keyPem, Buffer.from("not pem"))).toThrow(/no certificate/);
		expect(() => parseMaterial(leaf.certPem, leaf.keyPem, leaf.certPem)).toThrow(/non-CA/);
		const m = parseMaterial(leaf.certPem, leaf.keyPem, ca.pem);
		expect(peerPrincipal(m.leaf).serviceAccount).toBe("anvilkit-agent-api");
		for (const uris of [[], [apiURI, bwURI], ["https://x.invalid/a"]]) {
			const l = issue(ca, "x", uris);
			expect(() => peerPrincipal(new X509Certificate(l.certPem))).toThrow();
		}
	});

	it("refuses plaintext, a missing client certificate and a foreign CA at the handshake (AC1)", async () => {
		const ca = newCA("ca");
		const f = await startServer(ca.pem, serverLeaf(ca));
		try {
			const plain = health(`127.0.0.1:${f.port}`, credentials.createInsecure());
			expect(await code(check(plain))).toBe(status.UNAVAILABLE);
			plain.close();
			const noCert = health(
				`127.0.0.1:${f.port}`,
				credentials.createSsl(ca.pem),
				clientOptions("anvilkit-agent-knowledge"),
			);
			expect(await code(check(noCert))).toBe(status.UNAVAILABLE);
			noCert.close();
			const other = newCA("other");
			const foreign = client(f.port, clientDir(other, ca.pem, "anvilkit-agent-api", [apiURI]));
			expect(await code(check(foreign))).toBe(status.UNAVAILABLE);
			foreign.close();
			// the client refuses a server it does not trust (hostname verification intact)
			const distrust = client(f.port, clientDir(ca, other.pem, "anvilkit-agent-api", [apiURI]));
			expect(await code(check(distrust))).toBe(status.UNAVAILABLE);
			distrust.close();
			const wrongName = health(
				`127.0.0.1:${f.port}`,
				clientCredentials(new IdentityWatcher(files(clientDir(ca, ca.pem, "anvilkit-agent-api", [apiURI])), 50)),
				clientOptions("anvilkit-agent-mcp"),
			);
			expect(await code(check(wrongName))).toBe(status.UNAVAILABLE);
			wrongName.close();
			expect(f.handled()).toBe(0);
		} finally {
			f.server.forceShutdown();
			f.watcher.stop();
		}
	});

	it("authorizes by the peer's workload identity before the handler (AC2)", async () => {
		const ca = newCA("ca");
		const f = await startServer(ca.pem, serverLeaf(ca));
		try {
			const ok = client(f.port, clientDir(ca, ca.pem, "anvilkit-agent-api", [apiURI]));
			expect(await code(check(ok))).toBe("ok");
			ok.close();
			expect(f.handled()).toBe(1);
			for (const [name, uris] of [
				["unauthorized workload", [bwURI]],
				["wrong trust domain", [spiffe("other.invalid", "anvilkit-apps", "anvilkit-agent-api")]],
				["wrong namespace", [spiffe(td, "anvilkit-components", "anvilkit-agent-api")]],
			] as [string, string[]][]) {
				const c = client(f.port, clientDir(ca, ca.pem, "anvilkit-agent-api", uris));
				expect(await code(check(c)), name).toBe(status.PERMISSION_DENIED);
				c.close();
			}
			for (const [name, uris] of [
				["no URI SAN", []],
				["two URI SANs", [apiURI, bwURI]],
			] as [string, string[]][]) {
				const c = client(f.port, clientDir(ca, ca.pem, "anvilkit-agent-api", uris));
				expect(await code(check(c)), name).toBe(status.UNAUTHENTICATED);
				c.close();
			}
			expect(f.handled()).toBe(1);
		} finally {
			f.server.forceShutdown();
			f.watcher.stop();
		}
	});

	it("fails construction on an incomplete or stale policy", () => {
		const a = createAuthorizer({
			trustDomain: td,
			policy: { "/grpc.health.v1.Health/Check": [api], "/x.Y/Z": [api] },
			trustedCas: () => [],
		});
		expect(() => a.check(["/grpc.health.v1.Health/Check", "/grpc.health.v1.Health/Watch"])).toThrow(
			/Watch.*\/x\.Y\/Z/s,
		);
		expect(() => createAuthorizer({ trustDomain: "Bad_Domain", policy: {}, trustedCas: () => [] })).toThrow();
		expect(() => createAuthorizer({ trustDomain: td, policy: { "/a.B/C": [] }, trustedCas: () => [] })).toThrow();
		expect(
			createAuthorizer({ trustDomain: td, policy: { "/a.B/C": [anyAuthenticated] }, trustedCas: () => [] }).allowed(
				"/a.B/C",
				{ trustDomain: td, namespace: "x", serviceAccount: "y" },
			),
		).toBe(true);
	});

	it("rotates leaves and CAs at runtime, keeps valid material over invalid updates, retires trust and never resumes (AC4)", async () => {
		const ca1 = newCA("ca1");
		const ca2 = newCA("ca2");
		const both = Buffer.concat([ca1.pem, ca2.pem]);
		const f = await startServer(ca1.pem, serverLeaf(ca1));
		const waitReload = async (w: IdentityWatcher) => {
			const before = w.current().digest;
			const deadline = Date.now() + 3000;
			while (w.current().digest === before && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
			expect(w.current().digest).not.toBe(before);
		};
		try {
			const api1 = clientDir(ca1, ca1.pem, "anvilkit-agent-api", [apiURI]);
			const longLived = client(f.port, api1);
			expect(await code(check(longLived))).toBe("ok");

			// 1. server leaf and key replaced under the same CA
			mount(f.dir, serverLeaf(ca1), ca1.pem);
			await waitReload(f.watcher);
			let c = client(f.port, api1);
			expect(await code(check(c))).toBe("ok");
			c.close();
			expect(await code(check(longLived))).toBe("ok");

			// 2. client leaf replaced under the same CA
			mount(api1, issue(ca1, "anvilkit-agent-api", [apiURI]), ca1.pem);
			await new Promise((r) => setTimeout(r, 150));
			c = client(f.port, api1);
			expect(await code(check(c))).toBe("ok");
			c.close();

			// 3. transition: the server trusts both CAs and serves a ca2 leaf
			mount(f.dir, serverLeaf(ca2), both);
			await waitReload(f.watcher);
			expect(f.watcher.current().cas).toHaveLength(2);
			const api1both = clientDir(ca1, both, "anvilkit-agent-api", [apiURI]);
			const api2 = clientDir(ca2, ca2.pem, "anvilkit-agent-api", [apiURI]);
			c = client(f.port, api1both);
			expect(await code(check(c))).toBe("ok");
			c.close();
			c = client(f.port, api2);
			expect(await code(check(c))).toBe("ok");
			c.close();
			c = client(f.port, api1);
			expect(await code(check(c)), "a client trusting only ca1 refuses the ca2 server leaf").toBe(status.UNAVAILABLE);
			c.close();
			expect(await code(check(longLived)), "the long-lived ca1 connection survives while ca1 is trusted").toBe("ok");

			// 4. invalid updates leave the valid configuration active
			const before = f.watcher.current().digest;
			const bad = serverLeaf(ca2);
			bad.keyPem = issue(ca2, "other", [knowledgeURI]).keyPem;
			mount(f.dir, bad, both);
			const deadline = Date.now() + 3000;
			while (f.watcher.failures() === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
			expect(f.watcher.failures()).toBeGreaterThan(0);
			expect(f.watcher.current().digest).toBe(before);
			c = client(f.port, api2);
			expect(await code(check(c)), "the server keeps serving the last valid material").toBe("ok");
			c.close();
			rmSync(path.join(f.dir, "ca.crt"));
			await new Promise((r) => setTimeout(r, 150));
			expect(f.watcher.current().digest).toBe(before);
			symlinkSync(path.join("..data", "ca.crt"), path.join(f.dir, "ca.crt"));

			// 5. ca1 retired: new ca1 connections are refused, the long-lived ca1 connection is refused per call, ca2 keeps working
			mount(f.dir, serverLeaf(ca2), ca2.pem);
			await waitReload(f.watcher);
			expect(f.watcher.current().cas).toHaveLength(1);
			c = client(f.port, api1both);
			expect(await code(check(c))).toBe(status.UNAVAILABLE);
			c.close();
			expect(await code(check(longLived)), "retired trust on a long-lived connection").toBe(status.UNAUTHENTICATED);
			longLived.close();
			c = client(f.port, api2);
			expect(await code(check(c))).toBe("ok");
			c.close();

			// 6. a session resumed under retired trust gets no service. A raw TLS
			// client obtains a session while ca2 is trusted, ca2 is then retired
			// for ca3, and the client reconnects offering that session: either the
			// server resets the connection (TLS 1.3 completes the client's
			// handshake before the server's verdict, so the reset shows on the
			// first request) or the first RPC is refused per call; it is never
			// served. The same holds for a full handshake with the retired leaf.
			const ca3 = newCA("ca3");
			const leaf2 = issue(ca2, "anvilkit-agent-api", [apiURI]);
			const tickets: Buffer[] = [];
			// The HTTP/2 session is attached to the socket inside the connect
			// callback, before the server's first frames can be read by nobody.
			const dial = (session?: Buffer) =>
				new Promise<{ sock: TLSSocket; h2: ReturnType<typeof http2Connect> }>((resolve, reject) => {
					const sock = tlsConnect(
						{
							host: "127.0.0.1",
							port: f.port,
							ca: Buffer.concat([ca2.pem, ca3.pem]),
							cert: leaf2.certPem,
							key: leaf2.keyPem,
							servername: "anvilkit-agent-knowledge",
							minVersion: "TLSv1.3",
							ALPNProtocols: ["h2"],
							session,
						},
						() => resolve({ sock, h2: http2Connect(`https://127.0.0.1:${f.port}`, { createConnection: () => sock }) }),
					);
					sock.on("session", (ticket: Buffer) => tickets.push(ticket));
					sock.on("error", reject);
				});
			const outcome = async ({
				sock,
				h2,
			}: {
				sock: TLSSocket;
				h2: ReturnType<typeof http2Connect>;
			}): Promise<string> => {
				try {
					return await new Promise<string>((resolve) => {
						const timer = setTimeout(() => resolve("error timeout"), 10_000);
						const done = (v: string) => {
							clearTimeout(timer);
							resolve(v);
						};
						h2.on("error", (e: Error) => done(`error ${(e as NodeJS.ErrnoException).code ?? e.message}`));
						const req = h2.request({
							":method": "POST",
							":path": "/grpc.health.v1.Health/Check",
							"content-type": "application/grpc",
							te: "trailers",
						});
						const grpcStatus = (h: IncomingHttpHeaders) => {
							if (h["grpc-status"] !== undefined) done(`grpc-status ${String(h["grpc-status"])}`);
						};
						req.on("response", grpcStatus);
						req.on("trailers", grpcStatus);
						req.on("error", (e: Error) => done(`error ${(e as NodeJS.ErrnoException).code ?? e.message}`));
						req.end(Buffer.from([0, 0, 0, 0, 0]));
					});
				} finally {
					h2.close();
					sock.destroy();
				}
			};
			expect(await outcome(await dial()), "served while ca2 is trusted").toBe("grpc-status 0");
			const ticketDeadline = Date.now() + 2000;
			while (tickets.length === 0 && Date.now() < ticketDeadline) await new Promise((r) => setTimeout(r, 20));
			const session = tickets[0];
			mount(f.dir, issue(ca3, "anvilkit-agent-knowledge", [knowledgeURI], ["anvilkit-agent-knowledge"]), ca3.pem);
			await waitReload(f.watcher);
			for (const [label, sess] of [
				["full handshake", undefined],
				["resumed session", session],
			] as const) {
				let result: string;
				try {
					result = await outcome(await dial(sess as Buffer | undefined));
				} catch (e) {
					result = `error ${String(e)}`;
				}
				expect(result, label).not.toBe("grpc-status 0");
				expect(result, label).toMatch(/^error |^grpc-status 16$/);
			}
		} finally {
			f.server.forceShutdown();
			f.watcher.stop();
		}
	});
});
