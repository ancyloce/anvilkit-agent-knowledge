// P0.3: a request scope's roles are the user's verified roles only when the
// verified caller is the API workload; from any other workload (here the
// access sidecar, which may call Search) they are dropped before the
// application sees the scope.

import {
	RetrievalServiceClient,
	SearchRequest,
	type SearchResponse,
} from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { Metadata, type ServiceError } from "@grpc/grpc-js";
import { afterAll, describe, expect, it } from "vitest";
import type { Retrieval } from "../src/application/retrieval.js";
import type { Tasks } from "../src/application/tasks.js";
import { createGrpcServer, type GrpcServer } from "../src/transport/grpc.js";
import { clientCredentials, clientOptions, IdentityWatcher } from "../src/transport/identity.js";
import { policy } from "../src/transport/policy.js";
import { files, issue, mount, newCA, spiffe, tempDir } from "./pki.js";

const td = "anvilkit.local";
const servers: GrpcServer[] = [];
afterAll(async () => {
	for (const s of servers) await s.stop(1000);
});

describe("verified roles", () => {
	it("count only when the API workload carries them", async () => {
		const ca = newCA("roles-ca");
		const serverDir = tempDir();
		mount(
			serverDir,
			issue(
				ca,
				"anvilkit-agent-knowledge",
				[spiffe(td, "anvilkit-apps", "anvilkit-agent-knowledge")],
				["anvilkit-agent-knowledge"],
			),
			ca.pem,
		);
		const watcher = new IdentityWatcher(files(serverDir), 50);
		watcher.start();
		const seen: string[][] = [];
		const retrieval = {
			search: async (r: { scope: { roles?: string[] } }) => {
				seen.push(r.scope.roles ?? []);
				return { items: [], noAnswer: true, indexGeneration: 1, authorizationRevision: "1" };
			},
		} as unknown as Retrieval;
		// The allowlist of exactly the services this listener registers.
		const p = Object.fromEntries(
			Object.entries(policy()).filter(
				([m]) => !/\.(SourceService|SnapshotService|MemoryService|IngestService)\//.test(m),
			),
		);
		const log = { error: () => {}, info: () => {}, warn: () => {} };
		const server = createGrpcServer(
			"127.0.0.1:0",
			4,
			{} as Tasks,
			log,
			undefined,
			undefined,
			undefined,
			undefined,
			retrieval,
			undefined,
			undefined,
			undefined,
			undefined,
			{ watcher, trustDomain: td, policy: p },
		);
		servers.push(server);
		const port = await server.listen();
		server.serve();
		const search = (ns: string, sa: string): Promise<SearchResponse> => {
			const dir = tempDir();
			mount(dir, issue(ca, sa, [spiffe(td, ns, sa)]), ca.pem);
			const w = new IdentityWatcher(files(dir), 50);
			const client = new RetrievalServiceClient(
				`127.0.0.1:${port}`,
				clientCredentials(w),
				clientOptions("anvilkit-agent-knowledge"),
			);
			const req = SearchRequest.fromPartial({
				scope: { tenantId: "tenant_a", projectId: "proj_a", actorId: "user_a", roles: ["reviewer"] },
				snapshotId: "snap_1",
				query: "q",
				maxContextItems: 4,
				retrievalProfileId: "hybrid-v1",
				deadline: new Date(Date.now() + 60_000),
			});
			return new Promise((resolve, reject) =>
				client.search(req, new Metadata(), (err: ServiceError | null, res: SearchResponse) =>
					err ? reject(err) : resolve(res),
				),
			);
		};
		await search("anvilkit-apps", "anvilkit-agent-api");
		await search("anvilkit-components", "anvilkit-job-access-sidecar");
		expect(seen).toEqual([["reviewer"], []]);
	});
});
