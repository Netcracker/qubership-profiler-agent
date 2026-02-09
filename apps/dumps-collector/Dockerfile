# Build context: repository root
# Usage: docker build -f apps/dumps-collector/Dockerfile -t dumps-collector .

FROM golang:1.25.7-alpine3.22@sha256:20c8a94b529a9a127b6990c5e03537bd71ce3ebfdd744741e96168ac338bd862 AS builder

# Multi-arch build support
ARG TARGETOS=linux
ARG TARGETARCH

# renovate: datasource=maven depName=qubership-profiler-installer packageName=org.qubership.profiler:qubership-profiler-installer versioning=maven
ARG QUBERSHIP_PROFILER_INSTALLER_VERSION=3.1.1

# renovate: datasource=maven depName=qubership-profiler-diagtools packageName=org.qubership.profiler:qubership-profiler-diagtools versioning=maven
ARG QUBERSHIP_PROFILER_DIAGTOOLS_VERSION=3.1.1

ENV INSTALLER_CLOUD="https://repo1.maven.org/maven2/org/qubership/profiler/qubership-profiler-installer/${QUBERSHIP_PROFILER_INSTALLER_VERSION}/qubership-profiler-installer-${QUBERSHIP_PROFILER_INSTALLER_VERSION}.zip"

RUN apk --no-cache add \
     unzip \
     curl \
    && rm -rf /var/cache/apk/*

# Use repository root structure to match Go module paths
WORKDIR /workspace
RUN mkdir -p tools

# Copy go modules from repository root
COPY go.mod go.sum ./
RUN go mod download

# Copy go sources preserving the module structure
COPY apps/dumps-collector/ apps/dumps-collector/
COPY libs/ libs/

# Download installer
RUN curl -o installer_cloud.zip "${INSTALLER_CLOUD}"

# Download diagtools for the target architecture
RUN DIAGTOOLS_URL="https://repo1.maven.org/maven2/org/qubership/profiler/qubership-profiler-diagtools/${QUBERSHIP_PROFILER_DIAGTOOLS_VERSION}/qubership-profiler-diagtools-${QUBERSHIP_PROFILER_DIAGTOOLS_VERSION}-${TARGETOS}-${TARGETARCH}.tar.gz" \
    && curl -o diagtools.tar.gz "${DIAGTOOLS_URL}" \
    && tar -xzf diagtools.tar.gz \
    && cp diagtools-*/diag-bootstrap.sh /workspace/tools/ \
    && rm -rf diagtools.tar.gz diagtools-*

# Build from the app directory
RUN cd apps/dumps-collector && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -o /workspace/tools/prf_dump_writer main.go

FROM nginx:1.27.2-alpine3.20

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

COPY --from=builder --chown=${USER_UID} /workspace/installer_cloud.zip ${DOCROOT}/api/v1/diagnostic/agent/installer.zip

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
