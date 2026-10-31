# profiler-backend declares the SeaweedFS subchart in Chart.yaml, and Helm
# refuses to render the chart until the archive is in charts/, even with the
# subchart disabled. The archive is not committed, so fetch it here, at the
# version Chart.lock pins. The stage runs on the build host's platform: the
# archive is the same for every target platform.
FROM --platform=$BUILDPLATFORM alpine/helm:4.3.0@sha256:a6cf54599ccb99d90cf0712b30f03fdb3cab062e6b94e0418cc4db7e8a1464b2 AS chart-deps
COPY deploy/charts/profiler-backend /charts/profiler-backend
# `dependency build` resolves the URL in Chart.lock through a registered repository.
RUN helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm \
    && helm dependency build /charts/profiler-backend

FROM scratch
ARG USER_UID=1000

# Helm charts shipped for deployment
COPY deploy/charts/dumps-collector /charts/dumps-collector
COPY --from=chart-deps /charts/profiler-backend /charts/profiler-backend

USER ${USER_UID}
