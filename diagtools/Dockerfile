# syntax=docker/dockerfile:1@sha256:4a43a54dd1fedceb30ba47e76cfcf2b47304f4161c0caeac2db1c61804ea3c91

# Base image selection based on target OS
ARG BUILDPLATFORM
ARG TARGETOS=linux
FROM --platform=$BUILDPLATFORM golang:1.26.1-bookworm@sha256:8e8aa801e8417ef0b5c42b504dd34db3db911bb73dba933bd8bde75ed815fdbb AS builder-linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    gcc-x86-64-linux-gnu \
    libc6-dev-amd64-cross \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace

FROM --platform=$BUILDPLATFORM golang:1.26.1-bookworm@sha256:8e8aa801e8417ef0b5c42b504dd34db3db911bb73dba933bd8bde75ed815fdbb AS builder-windows
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
ARG TARGETOS
ARG TARGETARCH
ARG BUILDPLATFORM
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
