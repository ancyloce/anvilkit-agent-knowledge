# anvilkit-agent-knowledge: built from this repository alone (the build
# context is the repository root; nothing from the parent checkout is read).
# Two images from one file: the default target is the TypeScript service
# (the bundle with the generated contract consumers compiled in, its locked
# production dependencies, the reviewed secret-free configuration file); the
# `forwarder` target is the Go outbox forwarder sidecar (forwarder/), whose
# only inputs are the ANVILKIT_FORWARDER_* environment. No
# lifecycle script runs in the installs (pnpm-workspace.yaml). The
# anvilkit_knowledge migrations are applied by the parent repository's
# migration Job (jobs/migration) until Knowledge owns them; neither image
# runs DDL.
#   docker build -t anvilkit-agent-knowledge .
#   docker build --target forwarder -t anvilkit-knowledge-forwarder .
FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df AS build
WORKDIR /src
RUN npm install -g pnpm@12.3.4
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json build.mjs ./
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY src ./src
RUN pnpm run build
RUN pnpm install --frozen-lockfile --ignore-scripts --prod

FROM golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS forwarder-build
ARG GOPROXY=https://proxy.golang.org,direct
ARG GONOSUMDB=
ENV GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=0 GOPROXY=$GOPROXY GONOSUMDB=$GONOSUMDB
WORKDIR /src
COPY forwarder/go.mod forwarder/go.sum ./
RUN go mod download
COPY forwarder/cmd ./cmd
COPY forwarder/internal ./internal
RUN go build -trimpath -ldflags="-s -w" -o /out/anvilkit-knowledge-forwarder ./cmd/anvilkit-knowledge-forwarder

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS forwarder
COPY --from=forwarder-build /out/anvilkit-knowledge-forwarder /usr/local/bin/anvilkit-knowledge-forwarder
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/anvilkit-knowledge-forwarder"]

FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df
WORKDIR /anvilkit/knowledge
COPY --from=build /src/node_modules ./node_modules
COPY --from=build /src/dist ./dist
COPY package.json ./package.json
COPY config.yaml /etc/anvilkit/anvilkit-agent-knowledge/config.yaml
ENV ANVILKIT_KNOWLEDGE_CONFIG=/etc/anvilkit/anvilkit-agent-knowledge/config.yaml \
    NODE_ENV=production
USER 65532:65532
ENTRYPOINT ["node", "/anvilkit/knowledge/dist/main.js"]
