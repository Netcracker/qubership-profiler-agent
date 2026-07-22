# Build context: backend/ (the Go module root)
# Usage: docker buildx build --platform linux/arm64 --load \
#            -t cdt-load-generator:dev -f tools/load-generator/Dockerfile .
#
# The k6 binary is linked by k6runner with a plain `go build` (no xk6 CLI):
# the k6/x/cdt module and the go-prometheus-exporter live in this module, and
# the traffic is fully synthetic — no captured dumps enter the image.

FROM golang:1.26.2-alpine3.22@sha256:7ef941168f213aa115df2e61364d67682129e99dc8188b734139dea862cc7d31 AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY libs/ libs/
COPY tools/load-generator/ tools/load-generator/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s" -o /workspace/k6 ./tools/load-generator/k6runner

FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11 AS release

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
