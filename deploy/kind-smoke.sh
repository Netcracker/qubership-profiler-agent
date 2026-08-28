#!/usr/bin/env bash
# In-cluster smoke for charts/profiler-backend: build the image, load it into
# kind, install the chart with deploy/values-kind.yaml, assert the S3
# credentials are file-mounted (never env), then run the shared Stage 1 smoke
# (libs/tests/smoke) against port-forwarded services. The cold phase scales
# the collector StatefulSet to zero via the SMOKE_COLLECTOR_*_CMD hooks.
#
# Defaults target kind (CI); for OrbStack's k8s run:
#   KIND_CONTEXT=orbstack KIND_SKIP_LOAD=1 deploy/kind-smoke.sh
# (images come from the shared Docker daemon, so there is nothing to load).
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${KIND_CLUSTER:-profiler-smoke}"
CONTEXT="${KIND_CONTEXT:-kind-${CLUSTER}}"
RELEASE=profiler-backend
IMAGE=profiler-backend:dev
KUBECTL=(kubectl --context "${CONTEXT}")

echo "==> Building ${IMAGE}..."
docker build -f apps/profiler-backend/Dockerfile -t "${IMAGE}" .

if [[ "${CONTEXT}" == kind-* ]]; then
  if ! kind get clusters | grep -qx "${CLUSTER}"; then
    echo "==> Creating kind cluster ${CLUSTER}..."
    kind create cluster --name "${CLUSTER}" --wait 120s
  fi
  if [[ -z "${KIND_SKIP_LOAD:-}" ]]; then
    echo "==> Loading ${IMAGE} into kind..."
    kind load docker-image "${IMAGE}" --name "${CLUSTER}"
  fi
fi

echo "==> Installing the chart (fresh: old release and PVCs removed)..."
helm uninstall "${RELEASE}" --kube-context "${CONTEXT}" --wait 2>/dev/null || true
"${KUBECTL[@]}" delete pvc -l "app.kubernetes.io/instance=${RELEASE}" --ignore-not-found
helm install "${RELEASE}" charts/profiler-backend -f deploy/values-kind.yaml \
  --kube-context "${CONTEXT}" --wait --timeout 5m

echo "==> Asserting the S3 credentials are file-mounted, not env..."
# One kubectl call into a variable: with set -e a kubectl failure aborts here
# instead of feeding an empty stream into the greps.
env_names="$("${KUBECTL[@]}" get pods -l "app.kubernetes.io/instance=${RELEASE}" \
  -o jsonpath='{range .items[*].spec.containers[*].env[*]}{.name}{"\n"}{end}')"
if grep -Ex 'S3_(ACCESS|SECRET)_KEY' <<<"${env_names}"; then
  echo "FAIL: S3 credentials are injected as env; the Secret must mount as a volume (04 §6)" >&2
  exit 1
fi
if ! grep -qx 'S3_ACCESS_KEY_FILE' <<<"${env_names}"; then
  echo "FAIL: S3_ACCESS_KEY_FILE env is missing" >&2
  exit 1
fi

echo "==> Starting resilient port-forwards..."
# A previous run's kubectl children outlive their wrapper loops and would keep
# the localhost ports tunnelled into ANOTHER cluster — kill them first, and
# kill children (not just the loops) on exit for the same reason.
pkill -f "port-forward svc/${RELEASE}-" 2>/dev/null || true
sleep 1
PF_PIDS=()
forward() { # service local:remote — restarts when the target pod goes away
  while true; do
    "${KUBECTL[@]}" port-forward "svc/$1" "$2" >/dev/null 2>&1 || true
    sleep 1
  done
}
forward "${RELEASE}-collector-agent" 1715:1715 & PF_PIDS+=($!)
forward "${RELEASE}-collector-headless" 8081:8081 & PF_PIDS+=($!)
forward "${RELEASE}-query" 8080:8080 & PF_PIDS+=($!)
forward "${RELEASE}-minio" 9000:9000 & PF_PIDS+=($!)
cleanup() {
  kill "${PF_PIDS[@]}" 2>/dev/null || true
  pkill -f "port-forward svc/${RELEASE}-" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Running the Stage 1 smoke against the cluster..."
SMOKE_COLLECTOR_STOP_CMD="kubectl --context ${CONTEXT} scale statefulset/${RELEASE}-collector --replicas=0 \
  && kubectl --context ${CONTEXT} wait --for=delete pod/${RELEASE}-collector-0 --timeout=180s" \
SMOKE_COLLECTOR_START_CMD="kubectl --context ${CONTEXT} scale statefulset/${RELEASE}-collector --replicas=1" \
go test -tags smoke -count=1 -timeout 25m -v ./libs/tests/smoke/...

echo "==> kind smoke passed"
