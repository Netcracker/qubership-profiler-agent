# Build context: backend/ (the Go module root)
# Usage: docker build -f apps/profiler-backend/Dockerfile -t profiler-backend .
#
# One image, one static binary, one subcommand per workload
# (docs/design/04-storage-layout.md §2).

# The Stage 5 UI (apps/ui) builds in its own stage, so the image carries the
# /ui assets without a node toolchain on the host (07-ui-design.md §6).
FROM node:24-alpine AS ui-builder

WORKDIR /ui

COPY apps/ui/package.json apps/ui/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY apps/ui/ ./
# The product serves the SPA under /ui (07-ui-design.md §6); the query binary
# reads this base back out of the built index.html. Pass
# --build-arg UI_BASE_PATH=/ for a root build.
ARG UI_BASE_PATH=/ui/
ENV UI_BASE_PATH=$UI_BASE_PATH
RUN npm run build

FROM golang:1.26.4-alpine3.22@sha256:727cfc3c40be55cd1bc9a4a059406b28a059857e3be752aa9d09531e12c20c56 AS builder

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

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /workspace/profiler-backend /usr/local/bin/profiler-backend
COPY --from=builder --chown=65532:65532 /data /data

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/profiler-backend"]
