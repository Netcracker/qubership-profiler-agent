# Build context: backend/ (the Go module root)
# Usage: docker build -f apps/profiler-backend/Dockerfile -t profiler-backend .
#
# One image, one static binary, one subcommand per workload
# (docs/design/04-storage-layout.md §2).

# The Stage 5 UI (apps/ui) builds in its own stage, so the image carries the
# /ui assets without a node toolchain on the host (07-ui-design.md §6).
FROM node:24-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf AS ui-builder

WORKDIR /ui

COPY apps/ui/package.json apps/ui/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY apps/ui/ ./
RUN npm run build

FROM golang:1.27.0-alpine3.24@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY libs/ libs/
COPY apps/profiler-backend/ apps/profiler-backend/
COPY apps/ui/embed.go apps/ui/
COPY --from=ui-builder /ui/dist/ apps/ui/dist/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /workspace/profiler-backend ./apps/profiler-backend

# /data seeds the collector volume with the right owner: a named volume copies
# ownership from the image on first mount, and distroless has no shell to
# chown at run time.
RUN mkdir -p /data && chown 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=builder /workspace/profiler-backend /usr/local/bin/profiler-backend
COPY --from=builder --chown=65532:65532 /data /data

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/profiler-backend"]
