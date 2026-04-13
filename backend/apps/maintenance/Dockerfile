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
FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

ENV USER_UID=2001 \
    USER_NAME=maintenance_job \
    GROUP_NAME=maintenance_job

WORKDIR /

COPY --from=builder --chown=${USER_UID} /workspace/maintenance_job .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/maintenance_job"]
