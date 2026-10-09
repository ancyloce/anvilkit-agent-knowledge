// P0.6: the PostgreSQL URL alone makes node-postgres verify the server.
// pg 8.23 / pg-connection-string 2.14 turn sslmode=verify-full into
// ssl: { ca: <sslrootcert> }, i.e. Node's verifying defaults
// (rejectUnauthorized, the host name checked against the SANs); no pool in
// this package overrides ssl. For an IP host node-postgres passes no
// servername and Node checks the certificate against "localhost" instead of
// the IP, which is why the configuration refuses IP hosts outside
// development. A PostgreSQL double answers the SSLRequest with 'S' and then
// serves TLS, so the client's verdict is observable without a database: the
// server sees a completed handshake only when the client accepted its
// certificate.
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:net";
import path from "node:path";
import { TLSSocket } from "node:tls";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { type CA, issue, type Leaf, newCA, tempDir } from "./pki.js";

interface Double {
	port: number;
	handshakes: number;
	server: Server;
}

async function postgresDouble(leaf: Leaf, host: string): Promise<Double> {
	const d: Double = { port: 0, handshakes: 0, server: createServer() };
	d.server.on("connection", (socket) => {
		socket.on("error", () => undefined);
		// The 8-byte SSLRequest; 'S' accepts it and TLS follows.
		socket.once("data", () => {
			socket.write("S");
			const tls = new TLSSocket(socket, { isServer: true, cert: leaf.certPem, key: leaf.keyPem });
			tls.on("error", () => undefined);
			tls.once("secure", () => {
				d.handshakes++;
				tls.destroy();
			});
		});
	});
	await new Promise<void>((resolve, reject) => {
		d.server.once("error", reject);
		d.server.listen(0, host, () => resolve());
	});
	d.port = (d.server.address() as { port: number }).port;
	return d;
}

/** A leaf naming only db.anvilkit.test (pki.issue always adds localhost and 127.0.0.1). */
function dnsOnlyLeaf(ca: CA, dir: string): Leaf {
	const run = (args: string[]) => execFileSync("openssl", args, { cwd: dir, stdio: "ignore" });
	run(
		["req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"].concat([
			"-keyout",
			"db.key",
			"-out",
			"db.csr",
			"-subj",
			"/CN=db.anvilkit.test",
		]),
	);
	writeFileSync(path.join(dir, "db.ext"), "subjectAltName=DNS:db.anvilkit.test\nextendedKeyUsage=serverAuth\n");
	run(
		["x509", "-req", "-in", "db.csr", "-CA", path.join(ca.dir, "ca.crt"), "-CAkey", path.join(ca.dir, "ca.key")].concat(
			["-CAcreateserial", "-out", "db.crt", "-days", "1", "-extfile", "db.ext"],
		),
	);
	return { certPem: readFileSync(path.join(dir, "db.crt")), keyPem: readFileSync(path.join(dir, "db.key")) };
}

async function connectError(url: string): Promise<string> {
	const client = new pg.Client({ connectionString: url, connectionTimeoutMillis: 5000 });
	client.on("error", () => undefined);
	try {
		await client.connect();
		return "connected";
	} catch (err) {
		return err instanceof Error ? err.message : String(err);
	} finally {
		await client.end().catch(() => undefined);
	}
}

const refusedCertificate = /certificate|altnames|self.signed|unable to verify|issuer/i;

describe("node-postgres TLS from the URL (P0.6)", () => {
	const dir = tempDir();
	const ca = newCA("anvilkit-dev-postgres-ca");
	const other = newCA("someone-else");
	const caFile = path.join(dir, "ca.crt");
	const otherFile = path.join(dir, "other.crt");
	let local: Double;
	let dnsOnly: Double;
	let alias: Double | undefined;

	beforeAll(async () => {
		writeFileSync(caFile, ca.pem);
		writeFileSync(otherFile, other.pem);
		// This leaf names localhost and 127.0.0.1 only.
		const leaf = issue(ca, "anvilkit-postgres", []);
		local = await postgresDouble(leaf, "127.0.0.1");
		dnsOnly = await postgresDouble(dnsOnlyLeaf(ca, dir), "127.0.0.1");
		// 127.0.0.2 is loopback on Linux but not in the leaf's SANs.
		alias = await postgresDouble(leaf, "127.0.0.2").catch(() => undefined);
	});
	afterAll(() => {
		local?.server.close();
		dnsOnly?.server.close();
		alias?.server.close();
	});

	const url = (host: string, port: number, root: string) =>
		`postgres://u:p@${host}:${port}/db?sslmode=verify-full&sslrootcert=${encodeURIComponent(root)}`;

	it("completes the handshake with the CA of sslrootcert and the host named in the certificate", async () => {
		const before = local.handshakes;
		expect(await connectError(url("localhost", local.port, caFile))).not.toMatch(refusedCertificate);
		expect(local.handshakes).toBe(before + 1);
	});

	it("refuses a certificate of another CA", async () => {
		const before = local.handshakes;
		expect(await connectError(url("localhost", local.port, otherFile))).toMatch(refusedCertificate);
		expect(local.handshakes).toBe(before);
	});

	it("refuses a certificate that does not name the host", async () => {
		expect(await connectError(url("localhost", dnsOnly.port, caFile))).toMatch(/altnames/);
		expect(dnsOnly.handshakes).toBe(0);
	});

	it('checks an IP host against "localhost", not the IP (the configuration refuses IP hosts)', async (ctx) => {
		// The DNS-only leaf fails for 127.0.0.1 with Host: localhost in the error ...
		expect(await connectError(url("127.0.0.1", dnsOnly.port, caFile))).toMatch(/Host: localhost/);
		// ... and a leaf naming localhost passes for an IP it does not name.
		if (!alias) ctx.skip();
		const d = alias as Double;
		expect(await connectError(url("127.0.0.2", d.port, caFile))).not.toMatch(refusedCertificate);
		expect(d.handshakes).toBe(1);
	});
});
