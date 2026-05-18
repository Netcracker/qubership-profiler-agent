FROM grafana/xk6:1.4.3@sha256:4ef0baa8c724cd1c227d1f7d0e6a9eb8bf6c91e0c31b442903294d01499c2f01 as build
COPY --chmod=0777 . /tmp/proj

RUN mkdir -p /tmp/project
RUN cp -r /tmp/proj/* /tmp/project

WORKDIR /tmp/project

RUN xk6 build master \
    --with github.com/Netcracker/qubership-profiler-backend/apps/load-generator=/tmp/project \
    --with github.com/Netcracker/qubership-profiler-backend/apps/load-generator/go-metrics=/tmp/project/go-metrics

FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11 AS release

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
