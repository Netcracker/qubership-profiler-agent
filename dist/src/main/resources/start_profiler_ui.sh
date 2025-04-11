#!/bin/bash

JVM_ARGS="-Xmx512m"
CHECK_STARTED_DELAY_SECONDS=3
APP_CONF="applications/execution-statistics-collector/config/tomcat/application.properties"
JAR_FILE="applications/execution-statistics-collector/war/profiler.war"
PID_FILE="execution-statistics-collector/profiler_ui.pid"
ESC_FS_FOLDER="execution-statistics-collector"
DEFAULT_PORT=8180
PORT=''
PORT_OPENED=0
PID=0
PID_UP=0

function log() {
  echo $(date)" "$1
}

function fail() {
  message="ERROR: "$1
  log "$message"
  log "Failed to start ProfilerUI!"
  exit 1
}

function read_pid() {
  PID=0
  if [ -f $PID_FILE ]; then
    PID=$(cat $PID_FILE)
  fi
}

function read_port() {
  if [ -f $APP_CONF ]; then
    PORT=$(grep 'server.port' $APP_CONF | awk -F'=' '{print $2}' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  fi
  case $PORT in
    ''|*[!0-9]*) PORT=$DEFAULT_PORT ;;
    *) ;;
  esac
  log "ProfilerUI port="$PORT
}

function count_port_opened() {
  PORT_OPENED=$(netstat -plnt 2>/dev/null | awk '{ print $4 }' | grep ':'"$PORT"'$' -c)
}

function count_pid_up() {
  if [ $PID -gt 0 ]; then
    PID_UP=$(ps aux | awk '{ print $2 }' | grep $PID -c)
  else
    PID_UP=0
  fi
}

function check_port_is_used() {
  if [ $PORT_OPENED -gt 0 ]; then
    fail "Port "$PORT" is used."
  fi
}

function check_already_started() {
  if [ $PID_UP -gt 0 ]; then
    fail "Already started (PID="$PID")"
  fi
}

function check_up() {
  if [ $PID_UP -eq 0 ]; then
    fail "ProfilerUI failed to start. Please check logs."
  fi
}

function wait_port_opened() {
  timeout=15
  START_DATE=$(date +%s)
  while [ $PORT_OPENED -eq 0 ]; do
    log "Starting..."
    sleep $CHECK_STARTED_DELAY_SECONDS
    count_pid_up
    check_up
    count_port_opened
    CUR_DATE=$(date +%s)
    let duration=$CUR_DATE-$START_DATE
    if [ ${duration} -gt ${timeout} ]; then
		applications/execution-statistics-collector/stop_profiler_ui.sh
        fail "ProfilerUI failed to start. Please check logs."
    fi
  done
}

function start_profiler_ui() {
	nohup java $JVM_ARGS -Dprofiler_standalone_mode=true -jar $JAR_FILE > /dev/null 2>&1 & echo $! > $PID_FILE
}

function main() {
  log "ProfilerUI startup script"
  cd $(readlink -f "$0" | xargs dirname | xargs dirname | xargs dirname)
  mkdir -p $ESC_FS_FOLDER
  read_port
  count_port_opened
  check_port_is_used
  read_pid
  count_pid_up
  check_already_started
  start_profiler_ui
  read_pid
  wait_port_opened
  log "ProfilerUI successfully started."
}

main
