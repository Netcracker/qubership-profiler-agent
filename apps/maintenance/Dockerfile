# build
FROM golang:1.24.5-alpine3.22 as builder

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
FROM alpine:3.22.1

ENV USER_UID=2001 \
    USER_NAME=maintenance_job \
    GROUP_NAME=maintenance_job

WORKDIR /

COPY --from=builder --chown=${USER_UID} /workspace/maintenance_job .

RUN addgroup ${GROUP_NAME} && adduser -D -G ${GROUP_NAME} -u ${USER_UID} ${USER_NAME}
USER ${USER_UID}

ENTRYPOINT ["/maintenance_job"]
