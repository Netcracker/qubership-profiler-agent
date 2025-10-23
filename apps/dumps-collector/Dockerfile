FROM golang:1.24.5-alpine3.22 as builder

ENV TOOLS_VERSION="9.3.3.2.2" \
    INSTALLER_CLOUD="https://github.com/pd.saas.mvn/com/netcracker/execution-statistics-collector/installer-cloud" \
    PERF_TOOL="https://github.com/pd.saas.mvn/com/netcracker/execution-statistics-collector/prf-tool" \
    DIST="https://github.com/pd.saas.mvn/com/netcracker/execution-statistics-collector/dist"

RUN apk --no-cache add \
     unzip \
     curl \
    && rm -rf /var/cache/apk/*

WORKDIR /dump_collector
RUN mkdir tools
COPY go.mod go.sum ./
RUN go mod download

# copy go sources
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY main.go main.go

RUN curl -o installer_cloud.zip "${INSTALLER_CLOUD}/${TOOLS_VERSION}/installer-cloud-${TOOLS_VERSION}.zip" \
    && curl -o prf-tool.zip "${PERF_TOOL}/${TOOLS_VERSION}/prf-tool-${TOOLS_VERSION}.zip" \
    && curl -o /dump_collector/tools/execution-statistics-collector_autoinstaller.zip "${DIST}/${TOOLS_VERSION}/dist-${TOOLS_VERSION}-autoinstaller.zip" \
    && unzip installer_cloud.zip -d installer_cloud \
    && unzip prf-tool.zip \
    && cp /dump_collector/prf-tool/bin/prf /dump_collector/tools/ \
    && cp /dump_collector/prf-tool/bin/prf.exe /dump_collector/tools/ \
    && cp /dump_collector/prf-tool/bin/prf.mac /dump_collector/tools/ \
    && cp /dump_collector/installer_cloud/nc-diagnostic-bootstrap.sh /dump_collector/tools/ \
    && rm -rf prf-tool.zip installer_cloud prf_tool

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o /dump_collector/tools/prf_dump_writer main.go

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

COPY --from=builder --chown=${USER_UID} /dump_collector/tools ${DIAGNOSTIC_TOOLS_LOCATION}

COPY --from=builder --chown=${USER_UID} /dump_collector/installer_cloud.zip ${DOCROOT}/api/v1/diagnostic/agent/installer.zip

COPY static/index.html ${DIAGNOSTIC_TOOLS_LOCATION}/index.html

COPY src/nginx/nginx.conf /scripts/nginx/
COPY src/nginx/nginx-https.conf /scripts/nginx/

COPY src/nginx/entrypoint.sh /scripts/

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

CMD /scripts/entrypoint.sh
