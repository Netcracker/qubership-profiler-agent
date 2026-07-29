# Build context: backend/ (the Go module root)
# Usage: docker buildx build --platform linux/arm64 --load \
#            -t cdt-load-generator:dev -f tools/load-generator/Dockerfile .
#
# The k6 binary is linked by k6runner with a plain `go build` (no xk6 CLI):
# the k6/x/cdt module and the go-prometheus-exporter live in this module, and
# the traffic is fully synthetic — no captured dumps enter the image.

FROM golang:1.26.4-alpine3.22@sha256:727cfc3c40be55cd1bc9a4a059406b28a059857e3be752aa9d09531e12c20c56 AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY libs/ libs/
COPY tools/load-generator/ tools/load-generator/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s" -o /workspace/k6 ./tools/load-generator/k6runner

FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS release

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
