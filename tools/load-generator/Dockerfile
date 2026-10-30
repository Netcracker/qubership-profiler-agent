# Build context: the repository root (the Go module root)
# Usage: docker buildx build --platform linux/arm64 --load \
#            -t cdt-load-generator:dev -f tools/load-generator/Dockerfile .
#
# The k6 binary is linked by k6runner with a plain `go build` (no xk6 CLI):
# the k6/x/cdt module and the go-prometheus-exporter live in this module, and
# the traffic is fully synthetic — no captured dumps enter the image.

FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine3.24@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
COPY libs/ libs/
COPY tools/load-generator/ tools/load-generator/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s" -o /workspace/k6 ./tools/load-generator/k6runner

FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS release

COPY --from=builder /workspace/k6 /xk6/k6
COPY tools/load-generator/scripts /xk6/scripts
RUN chmod +x /xk6/k6 /xk6/scripts/start.sh

# 5656: the runner's own Go runtime metrics (go-prometheus-exporter).
# 6565: the k6 REST API the run orchestrator drives.
EXPOSE 5656
EXPOSE 6565

WORKDIR /xk6
USER 10001

ENTRYPOINT ["/xk6/scripts/start.sh"]
