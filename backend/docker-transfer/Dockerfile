FROM scratch
ARG USER_UID=1000

# Helm charts for dumps-collector deployment
COPY charts/dumps-collector /charts/dumps-collector
COPY charts/maintenance /charts/maintenance

USER ${USER_UID}
