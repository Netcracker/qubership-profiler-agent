#!/bin/bash

PID_FILE="execution-statistics-collector/profiler_ui.pid"
PID=0

function log() {
  echo $(date)" "$1
}

function read_pid() {
  PID=0
  if [ -f $PID_FILE ]; then
    PID=$(cat $PID_FILE)
  fi
}

function check_profiler_is_running() {
  if [ $PID -eq 0 ]; then
    log "ProfilerUI isn't running"
    exit
  fi
}

function stop_profiler_ui() {
	kill $PID
}

function delete_pid_file() {
	rm $PID_FILE
}

function main() {
	log "Stopping ProfilerUI..."
	cd $(readlink -f "$0" | xargs dirname | xargs dirname | xargs dirname)
	read_pid
	check_profiler_is_running
	stop_profiler_ui
	delete_pid_file
	log "ProfilerUI stopped."
}

main
