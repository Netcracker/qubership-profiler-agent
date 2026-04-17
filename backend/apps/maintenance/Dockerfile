# build
FROM golang:1.26.2-alpine3.22@sha256:c259ff7ffa06f1fd161a6abfa026573cf00f64cfd959c6d2a9d43e3ff63e8729 AS builder

# Multi-arch build support
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

# Copy go modules from repository root
COPY go.mod go.sum ./
RUN go mod download

# Copy go sources preserving the module structure
COPY apps/maintenance/ apps/maintenance/
COPY libs/ libs/

# Build from the app directory
RUN cd apps/maintenance && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -o /workspace/maintenance_job main.go

# Use alpine tiny images as a base
FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

ENV USER_UID=2001 \
    USER_NAME=maintenance_job \
    GROUP_NAME=maintenance_job

WORKDIR /

COPY --from=builder --chown=${USER_UID} /workspace/maintenance_job .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/maintenance_job"]
