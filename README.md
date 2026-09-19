# anvilkit-agent-knowledge

The Knowledge service of the AnvilKit Agent platform (architecture V4.0, [DD-07](https://github.com/ancyloce/anvilkit-services/blob/main/docs/architecture/knowledge.md), [DD-09](https://github.com/ancyloce/anvilkit-services/blob/main/docs/architecture/platform.md#async-config)). TypeScript (Node.js 24, grpc-js, pg) plus the Go outbox forwarder sidecar under `forwarder/`.

**Implemented (P14, durable background work):** the owner side of `anvilkit.knowledge.v1.BackgroundTaskService` — `ClaimTask`, `HeartbeatTask`, `SubmitTaskResult`, `GetTask` — over the `background_requests`/`task_attempts` tables of migration `00002` (still applied by the parent repository's migration Job, `jobs/migration`), the transactional outbox written on the same pg client transaction as every domain fact in the physical schema of the pinned watermill-sql v4.1.5 PostgreSQL adapter (`outbox`/`outbox_offsets`, `pg_current_xact_id()`, the watermill forwarder envelope), the Go forwarder sidecar that reads it with that adapter and publishes to NATS JetStream under the least-privilege `anvilkit_knowledge_forwarder` role, the lease sweeper with the Control original-dispatch query for external-effect leases, cancellation, immutable configuration generations with secret rotation (a new pool built and probed off-path, then swapped, then the old one drained) and the bounded lifecycle. Sources, ingestion, retrieval and MemoryFact (P15–P17) are not implemented; `local-check` is the DEVELOPMENT_ONLY fixture kind of the background lane.

## Layout

- `src/main.ts` — the service (probe listener first, gRPC listener, generation activation, lease sweeper, generation watcher; SIGTERM drains within bounds); `src/localcheck.ts` — the DEVELOPMENT_ONLY owner entry point (`request|cancel|get`) that creates fixture requests through the same code.
- `src/config.ts` — generations: defaults < `config.yaml` < validated Apollo snapshot < allowlisted `ANVILKIT_KNOWLEDGE_*` environment; `database.url` (or `database.url_file`) is the only secret.
- `src/domain/task.ts` — the request state machine and its decisions; `src/domain/event.ts` — the envelopes.
- `src/application/tasks.ts` — one transaction per command; the sweeper; `src/adapters/postgres.ts` — the client transactions, statements and the outbox insert; `src/adapters/control.ts` — the `GetDispatch` query.
- `src/transport/grpc.ts` — the listener with the contract's protovalidate boundary; `src/transport/health.ts` — `/healthz`, `/readyz`, `/metrics`.
- `forwarder/` — Go module `github.com/ancyloce/anvilkit-agent-knowledge/forwarder`: the watermill-sql subscriber, the watermill forwarder and the JetStream publisher, with its own configuration generations.
- `deploy/chart` — the Helm chart (the service plus the forwarder and the owner queue relay sidecars); `Dockerfile` — the service image and the `forwarder` target.

## Checks

`pnpm install --frozen-lockfile --ignore-scripts && pnpm run check-types && pnpm run lint && pnpm run build && pnpm test` (Vitest; Docker-backed PostgreSQL 17 through Testcontainers; the migrations are found beside this repository in the parent checkout or through `ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR`); in `forwarder/`: `GOWORK=off GOFLAGS=-mod=readonly go build ./... && go vet ./... && go test ./...` (a real NATS JetStream container and the Node rows of the built package); `docker build .`, `docker build --target forwarder .`, `helm lint deploy/chart`.

## Semantics the tests prove

The request and its `background.requested` event commit or roll back together; a new input supersedes the previous generation (stale, with its `background.completed`); two racing claims produce one claimant; a worker identity claims a generation once; a superseded, expired or fenced claimant's heartbeat and result are refused; an identical repeated submission returns the existing acceptance; input-digest mismatches change nothing; profile mismatches and reported failures consume the attempt within the bound; a deleted source cancels instead of accepting; an expired external-effect lease is released only with Control's not-sent evidence. The Go sidecar reads a row the Node owner wrote and publishes it to JetStream with `Nats-Msg-Id` = eventId; the forwarder identity cannot read domain tables. Startup failure unwinds the listeners; secret rotation replaces the pool and drains the old one; a rejected candidate leaves the active generation.
