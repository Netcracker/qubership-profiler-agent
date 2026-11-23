# Build context: repository root
# Usage: make -C apps/dumps-collector docker-build

FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine3.24@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder

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

FROM nginx:1.31.8-alpine3.24@sha256:fed7e14be83cbd2661a75cdc5b872d936cf155d46c327084d30b8df19ae88d72

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
