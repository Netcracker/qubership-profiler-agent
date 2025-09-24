#!/bin/bash

# To properly use this bootstrap, one needs
# 1. Download it using curl or wget http://${REMOTE_DUMP_HOST}:8080/api/v1/diagnostic/tools/diag-bootstrap.sh
# 2. Source it in local entry_point
# 3. Append X_JAVA_ARGS variable that it produces to java startup parameters
# 4. After java process finishes, call function send_crash_dump
# 5. Variable NC_DIAGNOSTIC_FOLDER has to be initialized with a path to folder where diagnostic agent should be unzipped

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

if [[ -z "${NC_DIAGNOSTIC_FOLDER}" ]]; then
  if [[ -z "${PROFILER_FOLDER}" ]]; then
    NC_DIAGNOSTIC_FOLDER="$(cd "$(realpath "$(dirname "${BASH_SOURCE[0]}")")" && pwd)"
    write_esc_log "NC_DIAGNOSTIC_FOLDER is not set by image. defaulting it to location of the bootstrap script ${NC_DIAGNOSTIC_FOLDER}"
    export NC_DIAGNOSTIC_FOLDER
  else
    write_esc_log "NC_DIAGNOSTIC_FOLDER is not set by image. defaulting it to legacy PROFILER_FOLDER value ${PROFILER_FOLDER}"
    export NC_DIAGNOSTIC_FOLDER="${PROFILER_FOLDER}"
  fi
else
  export NC_DIAGNOSTIC_FOLDER="${NC_DIAGNOSTIC_FOLDER}"
fi
mkdir -p "${NC_DIAGNOSTIC_FOLDER}"

# shellcheck disable=SC1090
. "${NC_DIAGNOSTIC_FOLDER}/diag-lib.sh"

if [ "true" = "${CONSUL_ENABLED}" ] || [ -n "${CONSUL_URL}" ] ; then
    config=consulCfg
elif [[ -n "${CONFIG_SERVER}" ]]; then
    config=serverCfg
elif [ "true" = "${ZOOKEEPER_ENABLED}" ]; then
    config=zkCfg
fi

if [[ -n "${NC_DIAGNOSTIC_CUSTOM_CONFIG}" ]]; then
  echo "${NC_DIAGNOSTIC_CUSTOM_CONFIG}" > "${NC_DIAGNOSTIC_FOLDER}/config/default/custom-config.xml"
elif [ -n "${config}" ] ; then
  "${NC_DIAGNOSTIC_FOLDER}"/diagtools "${config}" "${NC_DIAGNOSTIC_FOLDER}/properties" \
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
    "${NC_DIAGNOSTIC_FOLDER}"/diagtools schedule &
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
    if [ "${DOWNLOAD_SUCCESSFUL}" = "true" ] ; then
      unzip "-oq" -od "${NC_DIAGNOSTIC_FOLDER}" /tmp/installer.zip && rm /tmp/installer.zip
      write_esc_log "ESC agent has been downloaded"
      # shellcheck disable=SC1090
      . "${NC_DIAGNOSTIC_FOLDER}/diag-bootstrap.sh" UPDATED
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
