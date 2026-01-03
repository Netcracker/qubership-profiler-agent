# syntax=docker/dockerfile:1

# Base image selection based on target OS
# TARGETOS is automatically provided by BuildKit when using --platform
ARG TARGETOS
FROM golang:1.25.5-bookworm@sha256:09f53deea14d4019922334afe6258b7b776afc1d57952be2012f2c8c4076db05 AS builder-linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    gcc-x86-64-linux-gnu \
    libc6-dev-amd64-cross \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace

FROM golang:1.25.5-bookworm@sha256:09f53deea14d4019922334afe6258b7b776afc1d57952be2012f2c8c4076db05 AS builder-windows
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc-mingw-w64-x86-64 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace

# Select the appropriate builder based on TARGETOS
FROM builder-${TARGETOS} AS builder
WORKDIR /workspace

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies with cache mount
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY . .

# Build with cache mounts
# These ARGs are automatically provided by BuildKit when using --platform
ARG TARGETOS
ARG TARGETARCH
ARG BUILDARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    if [ "${TARGETOS}" = "windows" ]; then \
        CGO_ENABLED=1 GOOS=windows GOARCH=${TARGETARCH} CC=x86_64-w64-mingw32-gcc \
        go build -ldflags '-linkmode external -extldflags "-static"' -o diagtools.exe .; \
    else \
        if [ "${BUILDARCH}" != "${TARGETARCH}" ]; then \
            if [ "${TARGETARCH}" = "arm64" ]; then \
                export CC=aarch64-linux-gnu-gcc; \
            elif [ "${TARGETARCH}" = "amd64" ]; then \
                export CC=x86_64-linux-gnu-gcc; \
            fi; \
        fi; \
        CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -ldflags '-linkmode external -extldflags "-static"' -o diagtools .; \
    fi

# Export stage - just the binary in /out directory
FROM scratch AS export
COPY --from=builder /workspace/diagtools* /out/
