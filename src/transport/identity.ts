// Workload identity of the internal gRPC transport (P0.1, security.md
// SEC-01): a validating file watcher that feeds grpc-js's certificate
// provider credentials, the strict SPIFFE URI SAN of a peer, and the
// method→principal authorization that runs ahead of every handler. A
// workload certificate identifies a service; it never establishes a user,
// tenant, role or approver.
import { createHash, createPrivateKey, constants as cryptoConstants, X509Certificate } from "node:crypto";
import { readFileSync } from "node:fs";
import {
	type ChannelCredentials,
	type ChannelOptions,
	experimental,
	type ServerCredentials,
	ServerInterceptingCall,
	type ServerInterceptor,
	status,
} from "@grpc/grpc-js";

export interface Principal {
	trustDomain: string;
	namespace: string;
	serviceAccount: string;
}

export interface Workload {
	namespace: string;
	serviceAccount: string;
}

/** The policy entry of a method every verified workload may call. */
export const anyAuthenticated: Workload = { namespace: "*", serviceAccount: "*" };

/** Fully qualified method path (/package.Service/Method) → allowed workloads. */
export type Policy = Record<string, Workload[]>;

const label = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/;
const trustDomainPattern = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$/;

export function validTrustDomain(s: string): boolean {
	return trustDomainPattern.test(s);
}

export function principalString(p: Principal): string {
	return `spiffe://${p.trustDomain}/ns/${p.namespace}/sa/${p.serviceAccount}`;
}

/** Parses spiffe://<trust-domain>/ns/<namespace>/sa/<service-account> strictly. */
export function parseSpiffe(uri: string): Principal {
	let u: URL;
	try {
		u = new URL(uri);
	} catch {
		throw new Error(`uri ${JSON.stringify(uri)} is not a workload identity`);
	}
	if (u.protocol !== "spiffe:" || u.username || u.password || u.port || u.search || u.hash)
		throw new Error(`uri ${JSON.stringify(uri)} is not a workload identity`);
	if (!validTrustDomain(u.hostname)) throw new Error(`uri ${JSON.stringify(uri)} has no valid trust domain`);
	const parts = u.pathname.split("/");
	if (
		parts.length !== 5 ||
		parts[0] !== "" ||
		parts[1] !== "ns" ||
		parts[3] !== "sa" ||
		!label.test(parts[2] ?? "") ||
		!label.test(parts[4] ?? "")
	)
		throw new Error(`uri ${JSON.stringify(uri)} path is not /ns/<namespace>/sa/<service-account>`);
	return { trustDomain: u.hostname, namespace: parts[2] as string, serviceAccount: parts[4] as string };
}

/** The single URI SAN of a leaf certificate; none, several or a malformed one is no identity. */
export function peerPrincipal(leaf: X509Certificate): Principal {
	const uris = (leaf.subjectAltName ?? "")
		.split(", ")
		.filter((e) => e.startsWith("URI:"))
		.map((e) => e.slice(4));
	if (uris.length !== 1)
		throw new Error(`peer certificate carries ${uris.length} URI SANs, exactly one workload identity is required`);
	return parseSpiffe(uris[0] as string);
}

export interface Files {
	certFile: string;
	keyFile: string;
	caFile: string;
}

/** One complete, validated identity configuration; immutable. */
export interface Material {
	certPem: Buffer;
	keyPem: Buffer;
	caPem: Buffer;
	leaf: X509Certificate;
	cas: X509Certificate[];
	digest: string;
}

/** Validates the material as a whole: the key must match the certificate, the leaf must be valid now, the bundle must hold CA certificates only. */
export function parseMaterial(certPem: Buffer, keyPem: Buffer, caPem: Buffer, now = new Date()): Material {
	let leaf: X509Certificate;
	try {
		leaf = new X509Certificate(certPem);
	} catch (err) {
		throw new Error(`leaf: ${err instanceof Error ? err.message : String(err)}`);
	}
	let key: ReturnType<typeof createPrivateKey>;
	try {
		key = createPrivateKey(keyPem);
	} catch (err) {
		throw new Error(`key: ${err instanceof Error ? err.message : String(err)}`);
	}
	if (!leaf.checkPrivateKey(key)) throw new Error("certificate and key: key values mismatch");
	if (now < new Date(leaf.validFrom) || now > new Date(leaf.validTo))
		throw new Error(`leaf is not valid at ${now.toISOString()} (valid ${leaf.validFrom} to ${leaf.validTo})`);
	const cas: X509Certificate[] = [];
	for (const block of caPem.toString("utf8").split(/(?=-----BEGIN CERTIFICATE-----)/)) {
		if (!block.includes("-----BEGIN CERTIFICATE-----")) continue;
		let ca: X509Certificate;
		try {
			ca = new X509Certificate(block.trim());
		} catch (err) {
			throw new Error(`ca bundle: ${err instanceof Error ? err.message : String(err)}`);
		}
		if (!ca.ca) throw new Error(`ca bundle holds a non-CA certificate (${ca.subject})`);
		cas.push(ca);
	}
	if (cas.length === 0) throw new Error("ca bundle holds no certificate");
	const h = createHash("sha256");
	for (const b of [certPem, keyPem, caPem]) {
		h.update(b);
		h.update(Buffer.from([0]));
	}
	return { certPem, keyPem, caPem, leaf, cas, digest: h.digest("hex") };
}

export function loadMaterial(f: Files, now = new Date()): Material {
	return parseMaterial(readFileSync(f.certFile), readFileSync(f.keyFile), readFileSync(f.caFile), now);
}

/** Whether the leaf was issued and signed by one of the trusted CAs (direct issuance; the development and chart issuers are single-level). */
export function trusted(leaf: X509Certificate, cas: X509Certificate[]): boolean {
	return cas.some((ca) => leaf.checkIssued(ca) && leaf.verify(ca.publicKey));
}

/**
 * IdentityWatcher keeps the current Material of one set of files, polls
 * them for changes (mounted Secrets swap a ..data symlink; the files are
 * read by path every time) and feeds grpc-js's certificate-provider
 * credentials. A changed set is validated as a whole and published
 * atomically; an invalid candidate (a file missing mid-swap, a key that
 * does not match, an expired leaf, an empty bundle) is counted, logged and
 * leaves the current material in force. There is no fallback: construction
 * refuses invalid material.
 */
export class IdentityWatcher implements experimental.CertificateProvider {
	private material: Material;
	private timer?: NodeJS.Timeout;
	private readonly caListeners = new Set<experimental.CaCertificateUpdateListener>();
	private readonly identityListeners = new Set<experimental.IdentityCertificateUpdateListener>();
	private readonly changeListeners = new Set<(previous: Material, current: Material) => void>();
	private rejected = 0;

	constructor(
		private readonly files: Files,
		private readonly intervalMs = 5000,
		private readonly log: { warn(msg: string, f?: Record<string, string>): void } = { warn: () => {} },
	) {
		this.material = loadMaterial(files);
	}

	current(): Material {
		return this.material;
	}

	/** Rejected candidates since the last accepted one: the failure signal. */
	failures(): number {
		return this.rejected;
	}

	start(): void {
		if (this.timer) return;
		this.timer = setInterval(() => this.reload(), this.intervalMs);
		this.timer.unref();
	}

	stop(): void {
		if (this.timer) clearInterval(this.timer);
		this.timer = undefined;
	}

	onChange(fn: (previous: Material, current: Material) => void): void {
		this.changeListeners.add(fn);
	}

	/** Checks the files once; returns whether a new material was published. */
	reload(): boolean {
		let candidate: Material;
		try {
			const cert = readFileSync(this.files.certFile);
			const key = readFileSync(this.files.keyFile);
			const ca = readFileSync(this.files.caFile);
			const h = createHash("sha256");
			for (const b of [cert, key, ca]) {
				h.update(b);
				h.update(Buffer.from([0]));
			}
			if (h.digest("hex") === this.material.digest) return false;
			candidate = parseMaterial(cert, key, ca);
		} catch (err) {
			this.rejected++;
			this.log.warn("identity reload rejected; current material kept", {
				error: err instanceof Error ? err.message : String(err),
			});
			return false;
		}
		const previous = this.material;
		this.material = candidate;
		this.rejected = 0;
		this.publish();
		for (const fn of this.changeListeners) fn(previous, candidate);
		return true;
	}

	private publish(): void {
		for (const l of this.caListeners) l({ caCertificate: this.material.caPem });
		for (const l of this.identityListeners) l({ certificate: this.material.certPem, privateKey: this.material.keyPem });
	}

	// The initial update is delivered asynchronously: grpc-js subscribes to
	// the provider before it registers its own secure-context watcher, so a
	// synchronous callback would be published to nobody.
	addCaCertificateListener(listener: experimental.CaCertificateUpdateListener): void {
		this.caListeners.add(listener);
		queueMicrotask(() => {
			if (this.caListeners.has(listener)) listener({ caCertificate: this.material.caPem });
		});
	}

	removeCaCertificateListener(listener: experimental.CaCertificateUpdateListener): void {
		this.caListeners.delete(listener);
	}

	addIdentityCertificateListener(listener: experimental.IdentityCertificateUpdateListener): void {
		this.identityListeners.add(listener);
		queueMicrotask(() => {
			if (this.identityListeners.has(listener))
				listener({ certificate: this.material.certPem, privateKey: this.material.keyPem });
		});
	}

	removeIdentityCertificateListener(listener: experimental.IdentityCertificateUpdateListener): void {
		this.identityListeners.delete(listener);
	}
}

/**
 * Server credentials that require and verify client certificates against
 * the watcher's current bundle and present its current leaf; every update
 * the watcher publishes becomes the listener's secure context. Node keeps
 * a stateful TLS session cache that no public option disables, so a
 * resumed session is not prevented here: the authorizer re-verifies the
 * peer certificate against the current bundle on every call, which is
 * what refuses a session resumed under retired trust (pinned by test).
 * grpc-js 1.14 passes the first provider argument as the identity provider
 * despite its parameter name; the behavior is pinned by test as well.
 */
export function serverCredentials(watcher: IdentityWatcher): ServerCredentials {
	return experimental.createCertificateProviderServerCredentials(watcher, watcher, true);
}

/** Channel credentials presenting the watcher's current leaf and verifying the server against its current bundle. */
export function clientCredentials(watcher: IdentityWatcher): ChannelCredentials {
	return experimental.createCertificateProviderChannelCredentials(watcher, watcher, {});
}

/** The channel options of a client: standard hostname verification against the reviewed server name (a DNS SAN). */
export function clientOptions(serverName: string): ChannelOptions {
	if (!serverName) throw new Error("identity: server_name is required for hostname verification");
	return { "grpc.ssl_target_name_override": serverName };
}

export interface AuthorizerOptions {
	trustDomain: string;
	policy: Policy;
	/** The current trusted CAs, re-checked per call so a retired CA refuses the connections it had verified. */
	trustedCas: () => X509Certificate[];
}

export interface Authorizer {
	interceptor: ServerInterceptor;
	/** Lists registered methods without an entry and entries without a method; both are configuration errors. */
	check(registered: string[]): void;
	allowed(method: string, p: Principal): boolean;
}

export function createAuthorizer(o: AuthorizerOptions): Authorizer {
	if (!validTrustDomain(o.trustDomain))
		throw new Error(`identity: trust domain ${JSON.stringify(o.trustDomain)} is not a lowercase DNS name`);
	for (const [method, allowed] of Object.entries(o.policy)) {
		if (!method.startsWith("/") || method.split("/").length !== 3)
			throw new Error(`identity: policy method ${JSON.stringify(method)} is not /package.Service/Method`);
		if (allowed.length === 0) throw new Error(`identity: policy method ${method} allows nobody; remove it instead`);
		for (const w of allowed)
			if (w !== anyAuthenticated && (!label.test(w.namespace) || !label.test(w.serviceAccount)))
				throw new Error(
					`identity: policy method ${method} names an invalid workload ${w.namespace}/${w.serviceAccount}`,
				);
	}
	const allowed = (method: string, p: Principal): boolean =>
		p.trustDomain === o.trustDomain &&
		(o.policy[method] ?? []).some(
			(w) => w === anyAuthenticated || (w.namespace === p.namespace && w.serviceAccount === p.serviceAccount),
		);
	const decide = (method: string, raw: Buffer | undefined): { code: number; details: string } | undefined => {
		if (!raw)
			return { code: status.UNAUTHENTICATED, details: "UNAUTHENTICATED: no workload identity on this connection" };
		let leaf: X509Certificate;
		try {
			leaf = new X509Certificate(raw);
		} catch {
			return { code: status.UNAUTHENTICATED, details: "UNAUTHENTICATED: peer certificate unreadable" };
		}
		if (!trusted(leaf, o.trustedCas()))
			return { code: status.UNAUTHENTICATED, details: "UNAUTHENTICATED: the peer's trust was retired" };
		let p: Principal;
		try {
			p = peerPrincipal(leaf);
		} catch (err) {
			return {
				code: status.UNAUTHENTICATED,
				details: `UNAUTHENTICATED: ${err instanceof Error ? err.message : String(err)}`,
			};
		}
		if (!allowed(method, p))
			return {
				code: status.PERMISSION_DENIED,
				details: `PERMISSION_DENIED: ${principalString(p)} may not call ${method}`,
			};
		return undefined;
	};
	const interceptor: ServerInterceptor = (descriptor, call) =>
		new ServerInterceptingCall(call, {
			start: (next) => {
				const refusal = decide(descriptor.path, call.getAuthContext().sslPeerCertificate?.raw);
				if (refusal) call.sendStatus(refusal);
				else next();
			},
		});
	return {
		interceptor,
		allowed,
		check: (registered) => {
			const missing = registered.filter((m) => !(m in o.policy)).sort();
			const stale = Object.keys(o.policy)
				.filter((m) => !registered.includes(m))
				.sort();
			if (missing.length || stale.length)
				throw new Error(
					`identity: policy incomplete: methods without entry [${missing.join(", ")}]; entries without method [${stale.join(", ")}]`,
				);
		},
	};
}
