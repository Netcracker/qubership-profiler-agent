FROM grafana/xk6:1.2.5@sha256:f25a37ed80c913ce42fdba176b19d497bc1803596a1ce76b22d096f1529fcfdb as build
COPY --chmod=0777 . /tmp/proj

RUN mkdir -p /tmp/project
RUN cp -r /tmp/proj/* /tmp/project

WORKDIR /tmp/project

RUN xk6 build master \
    --with github.com/Netcracker/qubership-profiler-backend/apps/load-generator=/tmp/project \
    --with github.com/Netcracker/qubership-profiler-backend/apps/load-generator/go-metrics=/tmp/project/go-metrics

FROM alpine:3.18.2@sha256:82d1e9d7ed48a7523bdebc18cf6290bdb97b82302a8a9c27d4fe885949ea94d1 AS release

COPY --from=build /tmp/project/k6      /xk6/k6
COPY              scripts              /xk6/scripts
COPY              ./data               /xk6/data

ENV PODS=1
ENV DURATION=1m
ENV COLLECTOR_HOST=localhost
ENV EMULATOR_NAMESPACE=kTestNamespace
ENV EMULATOR_SERVICE_PREFIX=kTestService
ENV EMULATOR_POD_PREFIX=kTestServicePod

WORKDIR /xk6
RUN chmod +x /xk6
RUN chmod +x /xk6/k6
RUN dos2unix /xk6/scripts/start.sh
RUN chmod +x /xk6/scripts/start.sh

EXPOSE      80
EXPOSE      8080

USER 10001

ENTRYPOINT  [ "/xk6/scripts/start.sh" ]
