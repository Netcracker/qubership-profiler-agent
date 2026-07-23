FROM scratch
ARG USER_UID=1000

# Helm charts shipped for deployment
COPY charts/dumps-collector /charts/dumps-collector
COPY charts/profiler-backend /charts/profiler-backend

USER ${USER_UID}
