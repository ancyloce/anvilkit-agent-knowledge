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
# The runtime dependencies alone, installed in a directory of their own: a
# --prod install over the full one above removes nothing, and the image
# would ship every development package (vite, vitest, typescript, biome).
WORKDIR /prod
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --ignore-scripts --prod

FROM golang:1.27.3-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS forwarder-build
ARG GOPROXY=https://proxy.golang.org,direct
ARG GONOSUMDB=
ENV GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=0 GOPROXY=$GOPROXY GONOSUMDB=$GONOSUMDB
WORKDIR /src
COPY forwarder/go.mod forwarder/go.sum ./
RUN go mod download
COPY forwarder/cmd ./cmd
COPY forwarder/internal ./internal
RUN go build -trimpath -ldflags="-s -w" -o /out/anvilkit-knowledge-forwarder ./cmd/anvilkit-knowledge-forwarder

FROM alpine:3.24@sha256:3f4d6b32e26703c6082839234f43769d5046b3ac7c910033911d1e71d8cd630a AS forwarder
# Alpine zlib CVE-2026-4738: the base ships 1.3.2-r1 and v3.24/main serves
# the fixed 1.4-r0. A floor, not a pin: the build fails while the index
# lacks it, and drop this step once the alpine:3.24 pin ships >= 1.4-r0.
RUN apk add --no-cache 'zlib>=1.4-r0'
COPY --from=forwarder-build /out/anvilkit-knowledge-forwarder /usr/local/bin/anvilkit-knowledge-forwarder
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/anvilkit-knowledge-forwarder"]

FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df
WORKDIR /anvilkit/knowledge
# The runtime runs node only: the base image's npm, npx and corepack are
# removed so their bundled packages (and their advisories) are not shipped.
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
      /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
# Debian security fixes the base digest predates (with a fixed version:
# CRITICAL CVE-2026-13221/42496/8376 perl-base; HIGH
# CVE-2026-86145/89157/89161/103111 libpcre2, CVE-2026-109518 apt and its
# libapt-pkg), pinned to the fixed versions.
RUN apt-get update \
 && apt-get install -y --no-install-recommends --only-upgrade perl-base=5.36.0-7+deb12u4 libpcre2-8-0=10.42-1+deb12u2 \
      apt=2.6.2 libapt-pkg6.0=2.6.2 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /prod/node_modules ./node_modules
COPY --from=build /src/dist ./dist
COPY package.json ./package.json
COPY config.yaml /etc/anvilkit/anvilkit-agent-knowledge/config.yaml
ENV ANVILKIT_KNOWLEDGE_CONFIG=/etc/anvilkit/anvilkit-agent-knowledge/config.yaml \
    NODE_ENV=production
USER 65532:65532
ENTRYPOINT ["node", "/anvilkit/knowledge/dist/main.js"]
