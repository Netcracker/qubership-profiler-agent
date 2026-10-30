# Build context: repository root
# Usage: make -C apps/dumps-collector docker-build

FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine3.24@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder

# Multi-arch build support
ARG TARGETOS=linux
ARG TARGETARCH

# Use repository root structure to match Go module paths
WORKDIR /workspace
RUN mkdir -p tools

# Copy go modules and sources preserving the module structure
COPY go.mod go.sum ./
COPY apps/dumps-collector/ apps/dumps-collector/
COPY libs/ libs/

# Build from the app directory
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd apps/dumps-collector && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /workspace/tools/prf_dump_writer main.go

FROM nginx:1.31.7-alpine3.24@sha256:892bb2152464f0c5d8972593a5f5afaf8054552580b699d95f8ff4af08c2d33d

USER root

RUN apk --no-cache add \
        zip \
        unzip \
        coreutils \
    && rm -rf /var/cache/apk/*

ENV USER_UID=10001 \
    DOCROOT=/usr/share/docroot
ENV DIAGNOSTIC_TOOLS_LOCATION=${DOCROOT}/api/v1/diagnostic/tools/

COPY --from=builder --chown=${USER_UID} /workspace/tools ${DIAGNOSTIC_TOOLS_LOCATION}

COPY --chown=${USER_UID} apps/agent/installer/build/docker/installer.zip ${DOCROOT}/api/v1/diagnostic/agent/installer.zip
COPY --chown=${USER_UID} apps/agent/diagtools/scripts/diag-bootstrap.sh ${DIAGNOSTIC_TOOLS_LOCATION}/diag-bootstrap.sh

COPY apps/dumps-collector/static/index.html ${DIAGNOSTIC_TOOLS_LOCATION}/index.html

COPY apps/dumps-collector/src/nginx/nginx.conf /scripts/nginx/
COPY apps/dumps-collector/src/nginx/nginx-https.conf /scripts/nginx/

COPY apps/dumps-collector/src/nginx/entrypoint.sh /scripts/

RUN chown -R ${USER_UID} /scripts/ \
    && chmod -R a+rx /scripts/ \
    && mkdir -p \
        /diag \
        /var/www/tmp \
        /var/www/cache \
        /var/www/data \
    && chmod -R 777 \
        ${DOCROOT} \
        /diag \
        /var/log/nginx \
        /var/cache/nginx \
        /var/run \
        /usr/share/nginx/html \
        /etc/nginx \
        /var/www/tmp \
        /var/www/cache

USER ${USER_UID}

CMD ["/scripts/entrypoint.sh"]
