# syntax=docker/dockerfile:1@sha256:b6afd42430b15f2d2a4c5a02b919e98a525b785b1aaff16747d2f623364e39b6

# Base image selection based on target OS
ARG TARGETOS=linux
FROM golang:1.25.7-bookworm@sha256:38342f3e7a504bf1efad858c18e771f84b66dc0b363add7a57c9a0bbb6cf7b12 AS builder-linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace

FROM golang:1.25.7-bookworm@sha256:38342f3e7a504bf1efad858c18e771f84b66dc0b363add7a57c9a0bbb6cf7b12 AS builder-windows
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
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG BUILDARCH=amd64
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
