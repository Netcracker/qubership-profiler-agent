#!/bin/bash

# To properly use this bootstrap, one needs
# 1. Download it using curl or wget http://${REMOTE_DUMP_HOST}:8080/api/v1/diagnostic/tools/diag-bootstrap.sh
# 2. Source it in local entry_point
# 3. Append X_JAVA_ARGS variable that it produces to java startup parameters
# 4. After java process finishes, call function send_crash_dump
# 5. Variable NC_DIAGNOSTIC_HOME has to be initialized with a path to folder where diagnostic agent is unzipped
#    when this script is not in that folder

ALREADY_UPDATED="$1"

function diagnostic_logs_folder() {
  printf "%s" "${NC_DIAGNOSTIC_LOGS_FOLDER:-/tmp/diagnostic}"
}

# This one is needed for logging. so, create it as soon as possible
mkdir -p "$(diagnostic_logs_folder)/log"
esc_log_level="${ESC_LOG_LEVEL:-error}"

function diagnostic_log() {
  echo "$(diagnostic_logs_folder)/log/$(TZ=UTC date +%Y%m%d)".log
}
export -f diagnostic_log

write_esc_log () {
  if [ "$(type -t diagnostic_log)" = 'function' ]; then
    echo "$1" >> "$(diagnostic_log)"
  fi
  if [[ "${esc_log_level}" == "debug" ]] ; then
    echo "$1"
  fi
}
export -f write_esc_log

function download_file() {
  local filePath=$1
  local targetFile=$2
  if [[ "${REMOTE_STATIC_HOST}" != "" ]] ; then
    if [ "${TLS_ENABLED}" = "true" ]; then
      protocol="https"
    else
      protocol="http"
    fi
    local url="${protocol}://${REMOTE_STATIC_HOST}:8080${filePath}"
    write_esc_log "Try to download ESC agent from ${url}"

    local response_code="-1";
    response_code=$(curl  --connect-timeout 1 --write-out '%{http_code}' \
                      "${url}" --location --output "${targetFile}" "-s") || true
    if [[ "${response_code}" == "200" ]] ; then
      chmod a+rwx "${targetFile}"
      write_esc_log "INFO :${targetFile} has been downloaded"
    else
      write_esc_log "WARNING: Failed to download ${targetFile}"
      export DOWNLOAD_SUCCESSFUL=false;
    fi
  else
    write_esc_log "REMOTE_STATIC_HOST and REMOTE_DUMP_HOST are empty. can not download ${targetFile}"
    export DOWNLOAD_SUCCESSFUL=false;
  fi
}

write_esc_log "Start loading and initializing ESC agent scripts"

export DOWNLOAD_SUCCESSFUL=true;

# NC_DIAGNOSTIC_HOME is where the agent is installed: diag-lib.sh, diagtools, lib/agent.jar and config/.
# The image may keep it read-only.
# NC_DIAGNOSTIC_FOLDER is where the agent writes at runtime: properties/, localdump/, pod.name and the lock file.
# A container with a read-only root filesystem mounts an emptyDir or a tmpfs at /app/ncdiag for it.
bootstrap_folder="$(cd "$(realpath "$(dirname "${BASH_SOURCE[0]}")")" && pwd)"
if [[ -z "${NC_DIAGNOSTIC_HOME}" ]]; then
  if [[ -f "${bootstrap_folder}/diag-lib.sh" ]] && [[ -f "${bootstrap_folder}/lib/agent.jar" ]]; then
    NC_DIAGNOSTIC_HOME="${bootstrap_folder}"
  else
    # The bootstrap script is not next to the agent, so the agent is where the image unpacked it
    NC_DIAGNOSTIC_HOME="${NC_DIAGNOSTIC_FOLDER:-${PROFILER_FOLDER:-${bootstrap_folder}}}"
  fi
  write_esc_log "NC_DIAGNOSTIC_HOME is not set by image. defaulting it to ${NC_DIAGNOSTIC_HOME}"
fi
export NC_DIAGNOSTIC_HOME

diag_folder_defaulted=false
if [[ -z "${NC_DIAGNOSTIC_FOLDER}" ]]; then
  diag_folder_defaulted=true
  if [[ -n "${PROFILER_FOLDER}" ]]; then
    write_esc_log "NC_DIAGNOSTIC_FOLDER is not set by image. defaulting it to legacy PROFILER_FOLDER value ${PROFILER_FOLDER}"
    NC_DIAGNOSTIC_FOLDER="${PROFILER_FOLDER}"
  elif ! [[ -d /app/ncdiag && -w /app/ncdiag ]]; then
    write_esc_log "NC_DIAGNOSTIC_FOLDER is not set by image, and /app/ncdiag is not a writable folder. defaulting it to NC_DIAGNOSTIC_HOME ${NC_DIAGNOSTIC_HOME}"
    NC_DIAGNOSTIC_FOLDER="${NC_DIAGNOSTIC_HOME}"
  elif ! grep -q NC_DIAGNOSTIC_HOME "${NC_DIAGNOSTIC_HOME}/diag-lib.sh" 2>/dev/null; then
    # diag-lib.sh released before NC_DIAGNOSTIC_HOME expects the agent in NC_DIAGNOSTIC_FOLDER
    write_esc_log "NC_DIAGNOSTIC_FOLDER is not set by image, and ${NC_DIAGNOSTIC_HOME}/diag-lib.sh does not support a separate runtime folder. defaulting it to NC_DIAGNOSTIC_HOME ${NC_DIAGNOSTIC_HOME}"
    NC_DIAGNOSTIC_FOLDER="${NC_DIAGNOSTIC_HOME}"
  else
    write_esc_log "NC_DIAGNOSTIC_FOLDER is not set by image. defaulting it to /app/ncdiag"
    NC_DIAGNOSTIC_FOLDER=/app/ncdiag
    # properties/ caches settings from the config server. An emptyDir at /app/ncdiag outlives a container restart, and
    # a setting removed from the server has to stop applying, as it does when the folder is in the container layer.
    rm -rf "${NC_DIAGNOSTIC_FOLDER}/properties"
  fi
fi
export NC_DIAGNOSTIC_FOLDER
mkdir -p "${NC_DIAGNOSTIC_FOLDER}"

# The agent reads every file under the config/ folder next to _config.xml, and the custom configuration below is
# written into config/default/. NC_DIAGNOSTIC_HOME may be read-only, so the agent gets a copy of config/ in
# NC_DIAGNOSTIC_FOLDER, rebuilt on every start so it never keeps a custom configuration from an earlier one.
if [[ ! "${NC_DIAGNOSTIC_FOLDER}" -ef "${NC_DIAGNOSTIC_HOME}" ]] && [[ -d "${NC_DIAGNOSTIC_HOME}/config" ]]; then
  rm -rf "${NC_DIAGNOSTIC_FOLDER}/config"
  if ! cp -R "${NC_DIAGNOSTIC_HOME}/config" "${NC_DIAGNOSTIC_FOLDER}/config"; then
    write_esc_log "WARNING: failed to copy ${NC_DIAGNOSTIC_HOME}/config to ${NC_DIAGNOSTIC_FOLDER}/config. The agent reads its configuration from ${NC_DIAGNOSTIC_HOME}/config, and NC_DIAGNOSTIC_CUSTOM_CONFIG is not applied"
    rm -rf "${NC_DIAGNOSTIC_FOLDER}/config"
  fi
fi

# shellcheck disable=SC1090,SC1091
. "${NC_DIAGNOSTIC_HOME}/diag-lib.sh"

if [ "true" = "${CONSUL_ENABLED}" ] || [ -n "${CONSUL_URL}" ] ; then
    config=consulCfg
elif [[ -n "${CONFIG_SERVER}" ]]; then
    config=serverCfg
elif [ "true" = "${ZOOKEEPER_ENABLED}" ]; then
    config=zkCfg
fi

if [[ -n "${NC_DIAGNOSTIC_CUSTOM_CONFIG}" ]]; then
  echo "${NC_DIAGNOSTIC_CUSTOM_CONFIG}" > "${NC_DIAGNOSTIC_FOLDER}/config/default/custom-config.xml"
elif [ -n "${config}" ] && [ "${NC_DIAGNOSTIC_MODE}" != "off" ] ; then
  "${NC_DIAGNOSTIC_HOME}"/diagtools "${config}" "${NC_DIAGNOSTIC_FOLDER}/properties" \
      esc.config \
      NC_DIAGNOSTIC_ESC_ENABLED \
      NC_DIAGNOSTIC_ESC_BUFFERS \
      NC_DIAGNOSTIC_GC_ENABLED \
      NC_DIAGNOSTIC_TOP_ENABLED \
      NC_DIAGNOSTIC_THREADDUMP_ENABLED \
      NC_DIAGNOSTIC_DUMPS_ENABLED \
      DIAGNOSTIC_CENTER_DUMPS_ENABLED \
      PV_DIAGNOSTIC_DUMPS_ENABLED \
      NC_DIAGNOSTIC_AGENT_SERVICE \
      NC_DIAGNOSTIC_MODE
fi

if [[ -z ${NC_DIAGNOSTIC_MODE} ]] ; then
  if [ "${PROFILER_ENABLED}" = "true" ] ; then
    export NC_DIAGNOSTIC_MODE="dev"
    write_esc_log "NC_DIAGNOSTIC_MODE variable is empty but legacy PROFILER_ENABLED is specified to be ${PROFILER_ENABLED}. Populating NC_DIAGNOSTIC_MODE with '${NC_DIAGNOSTIC_MODE}'"
  elif [ -n "${PROFILER_ENABLED}" ] ; then
    export NC_DIAGNOSTIC_MODE="off"
    write_esc_log "NC_DIAGNOSTIC_MODE variable is empty but legacy PROFILER_ENABLED is specified to be ${PROFILER_ENABLED}. Populating NC_DIAGNOSTIC_MODE with '${NC_DIAGNOSTIC_MODE}'"
  else
    export NC_DIAGNOSTIC_MODE="prod"
    write_esc_log "NC_DIAGNOSTIC_MODE and PROFILER_ENABLED variables are empty. Defaulting NC_DIAGNOSTIC_MODE to '${NC_DIAGNOSTIC_MODE}'"
  fi
fi

# this function added to support different patterns of kubernetes service urls
# Function to extract and transform the base service name from different types of urls
# Ex.
# 1. http://esc-collector-service:8080 or
#    http://esc-static-service:8080 or
#    esc-collector-service => esc-collector-service
# 2. esc-static-service.profiler.svc => esc-collector-service.profiler.svc
# 3. http://nc-diagnostic-agent.namespace:8080 => nc-diagnostic-agent.namespace
transform_service_name() {
  local service_url="$1"

  # Extract base service name, removing protocol, port, and namespace
  local base_service
  base_service=$(echo "$service_url" | sed -E 's@^https?://@@' | sed -E 's/:.*$//')

  # REMOTE_DUMP_HOST may contain either esc-static-service address or esc-collector-service address
  # Need to auto-detect both of them and use accordingly
  echo "${base_service//esc-static-service/esc-collector-service}"
}

if [ "dev" = "${NC_DIAGNOSTIC_MODE}" ] || [ "prod" = "${NC_DIAGNOSTIC_MODE}" ] ; then
  if [ -z "${NC_DIAGNOSTIC_AGENT_SERVICE}" ] ; then
    if [ -n "${REMOTE_DUMP_HOST}" ] ; then
      export NC_DIAGNOSTIC_AGENT_SERVICE="${REMOTE_DUMP_HOST}"
      write_esc_log "NC_DIAGNOSTIC_AGENT_SERVICE is empty. Populating it from legacy REMOTE_DUMP_HOST: '${NC_DIAGNOSTIC_AGENT_SERVICE}'"
    else
      export NC_DIAGNOSTIC_AGENT_SERVICE="nc-diagnostic-agent"
      write_esc_log "NC_DIAGNOSTIC_AGENT_SERVICE and REMOTE_DUMP_HOST are empty. Defaulting NC_DIAGNOSTIC_AGENT_SERVICE to '${NC_DIAGNOSTIC_AGENT_SERVICE}'"
    fi
  fi

  REMOTE_DUMP_HOST=$(transform_service_name "$NC_DIAGNOSTIC_AGENT_SERVICE")
  REMOTE_STATIC_HOST="${REMOTE_DUMP_HOST}"
  export REMOTE_STATIC_HOST
  export REMOTE_DUMP_HOST
fi

function enable_diag() {
  write_esc_log "Initializing diagnostic log to $(diagnostic_log)"
  write_esc_log "Diagnostic enabled: $(diagnostic_enabled)"

  apply_esc_configuration_to_X_JAVA_ARGS

  if diagnostic_enabled ; then
    "${NC_DIAGNOSTIC_HOME}"/diagtools schedule &
  fi
}

if [ "dev" = "${NC_DIAGNOSTIC_MODE}" ] ; then
  if [ "${ALREADY_UPDATED}" = "UPDATED" ] ; then
    write_esc_log "Update for DEV diagnostic mode already performed. Proceeding with the bootstrap"
    enable_diag
  else
    write_esc_log "Updating ESC agent since DEV mode is specified"
    export DOWNLOAD_SUCCESSFUL=true
    download_file "/api/v1/diagnostic/agent/updates?forMD5=" "/tmp/installer.zip"
    # The update replaces the installed agent, so it fails when NC_DIAGNOSTIC_HOME is read-only
    if [ "${DOWNLOAD_SUCCESSFUL}" = "true" ] && unzip "-oq" -od "${NC_DIAGNOSTIC_HOME}" /tmp/installer.zip ; then
      rm -f /tmp/installer.zip
      write_esc_log "ESC agent has been downloaded"
      # The collector may serve an agent.jar released before NC_DIAGNOSTIC_HOME, which runs diagtools from
      # NC_DIAGNOSTIC_FOLDER. NC_DIAGNOSTIC_HOME is writable here, so it keeps the runtime data too.
      if [ "${diag_folder_defaulted}" = "true" ] ; then
        export NC_DIAGNOSTIC_FOLDER="${NC_DIAGNOSTIC_HOME}"
      fi
      # shellcheck disable=SC1090,SC1091
      . "${NC_DIAGNOSTIC_HOME}/diag-bootstrap.sh" UPDATED
    elif [ "${DOWNLOAD_SUCCESSFUL}" = "true" ] ; then
      rm -f /tmp/installer.zip
      write_esc_log "Failed to unpack the ESC agent update into ${NC_DIAGNOSTIC_HOME}. Agent will run with PROD profile."
      export NC_DIAGNOSTIC_MODE="prod"
    else
      write_esc_log "Failed to update ESC agent in DEV profile. Agent will run with PROD profile."
      export NC_DIAGNOSTIC_MODE="prod"
    fi
  fi
fi

if [ "prod" = "${NC_DIAGNOSTIC_MODE}" ] ; then
  write_esc_log "Initializing PROD diagnostic mode"
  enable_diag
else
  write_esc_log "NC_DIAGNOSTIC_MODE is ${NC_DIAGNOSTIC_MODE}. Skipping initialization of ESC agent"
fi
