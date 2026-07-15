#!/usr/bin/env bash
#
# run-agent.sh — build the real Java profiler agent + the adversarial test-app,
# then run the app under -javaagent so it streams profiling data to a running
# Go collector over TCP.
#
# This is the "produce data" half of the real-agent E2E. The "assert data" half
# lives in backend/libs/tests/smoke_realagent/realagent_test.go, which shells
# out to this script and then queries the backend HTTP API.
#
# It does NOT stand up the backend — the caller (the Go test / the Makefile
# target) is expected to have `docker compose up` running with the collector
# reachable at ${COLLECTOR_HOST}:${COLLECTOR_PORT}.
#
# Environment:
#   COLLECTOR_HOST   collector TCP host   (default: localhost)
#   COLLECTOR_PORT   collector TCP port   (default: 1715)
#   CLOUD_NAMESPACE  pod namespace tag    (default: e2e-realagent)
#   MICROSERVICE_NAME service tag         (default: adversarial-app)
#   SKIP_BUILD       set to 1 to reuse an already-built profiler-home + jar
#
# The agent switches from local-file dumps to the TCP collector purely because
# REMOTE_DUMP_HOST is set (see dumper/.../Dumper.java: remoteConfigured =
# isNotEmpty(REMOTE_DUMP_HOST); localDumpEnabled = !remoteConfigured). The plain
# port defaults to ProtocolConst.PLAIN_SOCKET_PORT = 1715 when REMOTE_DUMP_PORT_SSL
# is unset, so we pass REMOTE_DUMP_PORT_PLAIN explicitly for clarity.
set -euo pipefail

COLLECTOR_HOST="${COLLECTOR_HOST:-localhost}"
COLLECTOR_PORT="${COLLECTOR_PORT:-1715}"
CLOUD_NAMESPACE="${CLOUD_NAMESPACE:-e2e-realagent}"
MICROSERVICE_NAME="${MICROSERVICE_NAME:-adversarial-app}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROFILER_HOME="${REPO_ROOT}/installer-zip-test/build/profiler-home"
AGENT_JAR="${PROFILER_HOME}/lib/qubership-profiler-agent.jar"
CONFIG="${SCRIPT_DIR}/config/_config.xml"

echo "[run-agent] repo root:      ${REPO_ROOT}"
echo "[run-agent] collector:      ${COLLECTOR_HOST}:${COLLECTOR_PORT}"

cd "${REPO_ROOT}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "[run-agent] building the agent (installer zip) and the test-app jar..."
  # extractInstaller unpacks the installer zip into installer-zip-test/build/profiler-home,
  # which is exactly the lib/ + config/ layout a deployed agent uses.
  ./gradlew --quiet :installer-zip-test:extractInstaller :test-app:jar
fi

if [[ ! -f "${AGENT_JAR}" ]]; then
  echo "[run-agent] ERROR: agent jar not found at ${AGENT_JAR}" >&2
  echo "[run-agent] run without SKIP_BUILD=1, or build it first." >&2
  exit 1
fi
# The test-app jar carries a build version in its name (qubership-profiler-test-app-<ver>.jar),
# so resolve it by glob rather than a pinned path.
TESTAPP_JAR="$(find "${REPO_ROOT}/test-app/build/libs" -maxdepth 1 -name 'qubership-profiler-test-app-*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' 2>/dev/null | sort | tail -1 || true)"
if [[ -z "${TESTAPP_JAR}" || ! -f "${TESTAPP_JAR}" ]]; then
  echo "[run-agent] ERROR: test-app jar not found under ${REPO_ROOT}/test-app/build/libs" >&2
  exit 1
fi

echo "[run-agent] agent jar:      ${AGENT_JAR}"
echo "[run-agent] test-app jar:   ${TESTAPP_JAR}"
echo "[run-agent] config:         ${CONFIG}"
echo "[run-agent] running the adversarial workload under -javaagent..."

# -Dfile.encoding=UTF-8 keeps the adversarial source literals intact on JVMs
# whose platform default is not UTF-8.
# The agent auto-detects its plugin jars from ${profiler.home}/lib. We point
# profiler.home at the extracted installer profiler-home (which ships lib/), but
# keep profiler.config on our in-tree adversarial config. Without an explicit
# profiler.home the agent would derive it as the grandparent of the config file
# (Bootstrap/DumpRootResolverAgent), which has no lib/ and fails to load plugins.
set -x
java \
  -Dfile.encoding=UTF-8 \
  -javaagent:"${AGENT_JAR}" \
  -Dprofiler.home="${PROFILER_HOME}" \
  -Dprofiler.config="${CONFIG}" \
  -DREMOTE_DUMP_HOST="${COLLECTOR_HOST}" \
  -DREMOTE_DUMP_PORT_PLAIN="${COLLECTOR_PORT}" \
  -DCLOUD_NAMESPACE="${CLOUD_NAMESPACE}" \
  -DMICROSERVICE_NAME="${MICROSERVICE_NAME}" \
  -cp "${TESTAPP_JAR}" \
  com.netcracker.profilerTest.testapp.AdversarialMain
set +x

echo "[run-agent] workload finished; data streamed to ${COLLECTOR_HOST}:${COLLECTOR_PORT}."
