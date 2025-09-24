#!/bin/bash

# Return option value
# <name of option> <default value>
function get_option_value() {
  if [[ -z "${!1}" ]] ; then
    configFile="${NC_DIAGNOSTIC_FOLDER}/properties/${1}"
    if [[ -s "$configFile" ]] ; then
      cat "$configFile"
    elif [[ -n "$2" ]] ; then
      echo "$2"
    fi
  else
    echo "${!1}"
  fi
}
export -f get_option_value

function check_option_not_disabled() {
  OPTION_VALUE=$(get_option_value "${1}");
  if [[ "${OPTION_VALUE}" == "false" ]] ; then
    return 1
  else
    return 0
  fi
}
export -f check_option_not_disabled

# This function adds necessary java startup parameters to X_JAVA_ARGS variable
function apply_esc_configuration_to_X_JAVA_ARGS() {
  local java_agent=""
  if check_option_not_disabled NC_DIAGNOSTIC_ESC_ENABLED ; then
    write_esc_log 'Attaching execution statistics collector'

    local local_dump_location="${NC_DIAGNOSTIC_FOLDER}/localdump"
    write_esc_log "creating directory for local dump: ${local_dump_location}"
    mkdir -p "${local_dump_location}"

    local esc_buffers
    esc_buffers="$(get_option_value NC_DIAGNOSTIC_ESC_BUFFERS 50)"
    # NC_DIAGNOSTIC_ESC_TAG_LIMIT should be in range 100..5100 (or -1)
    local esc_tag_limit
    esc_tag_limit="$(get_option_value NC_DIAGNOSTIC_ESC_TAG_LIMIT 5000)"
    java_agent="${java_agent}-javaagent:${NC_DIAGNOSTIC_FOLDER}/lib/agent.jar"
    java_agent="${java_agent} -Dcom.netcracker.profiler.agent.Profiler.MAX_BUFFERS=${esc_buffers}"
    java_agent="${java_agent} -Dcom.netcracker.profiler.agent.Profiler.INITIAL_BUFFERS=${esc_buffers}"
    java_agent="${java_agent} -Dcom.netcracker.profiler.agent.Profiler.DICTIONARY_TAG_TRIM_SIZE=${esc_tag_limit}"
    java_agent="${java_agent} -Dprofiler.dump=${local_dump_location}"
  else
    write_esc_log 'Execution statistics collector is disabled and would not be attached'
  fi

  local prf_options=""
  if diagnostic_enabled && check_option_not_disabled NC_DIAGNOSTIC_DUMPS_ENABLED ; then
    prf_options="${prf_options} -XX:+HeapDumpOnOutOfMemoryError"
    prf_options="${prf_options} -XX:HeapDumpPath=$(diagnostic_logs_folder)"
  fi

  if [[ -z "${X_JAVA_ARGS}" ]] ; then
    X_JAVA_ARGS=""
  fi

  local javac_version
  javac_version=$(get_javac_version)

  # If Java version 11 or high we should use -xlog parameters
  if version_ge "${javac_version}" "11.0.0"; then
    X_JAVA_ARGS="${java_agent} $(gc_log_opts_for_jdk_11_and_high) ${prf_options} ${X_JAVA_ARGS}"
  else
    X_JAVA_ARGS="${java_agent} $(gc_log_opts_for_jdk_7_and_less) ${prf_options} ${X_JAVA_ARGS}"
  fi

  export X_JAVA_ARGS
  write_esc_log "resulting X_JAVA_ARGS ${X_JAVA_ARGS}"
}
export -f apply_esc_configuration_to_X_JAVA_ARGS

function get_javac_version() {
  printf '%s' "$(java -version 2>&1 | awk -F '\"' '/version/ {print $2}')"
}
export -f get_javac_version

function current_timestamp() {
  TZ=UTC date +"%Y%m%dT%H%M%S"
}
export -f current_timestamp

function gc_log_file_size() {
  printf '%s' "${GC_LOG_FILE_SIZE:-10000K}"
}

function gc_log_number_of_files() {
  printf '%s' "${GC_LOG_NUMBER_OF_FILES:-10}"
}

# Return JVM parameters for JDK < 11.x (usually JDK 1.7)
function gc_log_opts_for_jdk_7_and_less() {
  if check_option_not_disabled NC_DIAGNOSTIC_GC_ENABLED ; then
    mkdir -p "$(diagnostic_logs_folder)/gclogs"
    echo -n "-verbose:gc " \
            "-XX:+PrintGCDetails " \
            "-XX:+PrintGCDateStamps " \
            "-Xloggc:$(diagnostic_logs_folder)/gclogs/gc.log " \
            "-XX:+UseGCLogFileRotation " \
            "-XX:GCLogFileSize=$(gc_log_file_size) " \
            "-XX:-UseGCOverheadLimit " \
            "-XX:NumberOfGCLogFiles=$(gc_log_number_of_files)" \
            "-DESC_HARVEST_GCLOG=true"
  fi
}

# Return JVM parameters for JDK >= 11.x (usually JDK 11.x and 17.x or high)
# In accordance with https://githubmemory.com/repo/chewiebug/GCViewer/issues/251
function gc_log_opts_for_jdk_11_and_high() {
  if check_option_not_disabled NC_DIAGNOSTIC_GC_ENABLED ; then
    mkdir -p "$(diagnostic_logs_folder)/gclogs"
    echo -n "-XX:-UseGCOverheadLimit" \
            "-Xlog:gc=trace:file=$(diagnostic_logs_folder)/gclogs/gc.log:tags,time,uptime,level:filecount=$(gc_log_number_of_files),filesize=$(gc_log_file_size)" \
            "-DESC_HARVEST_GCLOG=true"
  fi
}

function diagnostic_enabled() {
  if diagnostic_center_enabled ; then
    return  0
  else
    return 1
  fi
}
export -f diagnostic_enabled

function diagnostic_center_enabled() {
  if check_option_not_disabled DIAGNOSTIC_CENTER_DUMPS_ENABLED ; then
    return 0
  else
    return 1
  fi
}
export -f diagnostic_center_enabled

# Send collected HeapDumps to ESC/CDT
send_crash_dump() {
  export ESC_LOG_LEVEL=debug
  export esc_log_level=debug

   write_esc_log 'start to send crash dump'

  "${NC_DIAGNOSTIC_FOLDER}"/diagtools scan "$(diagnostic_logs_folder)"/*.hprof* ./core* ./hs_err*
}
export -f send_crash_dump

# These functions allow to compare java version with using natural sort
# For example:
#  if version_ge "17.0.3" "11.0.0"; then
#    echo "Version 17.0.3 is greater than or equal to 11.0.0"
#  fi

# The function compare V1 and V2 and return true if V1 > V2
function version_ge() {
  test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1";
}
