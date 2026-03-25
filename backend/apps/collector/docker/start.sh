#!/bin/bash

java_debug() {
  local options=""

  if [ -n "$JAVA_DEBUG" ]; then
    options="-agentlib:jdwp=transport=dt_socket,server=y,address=*:5005"
    if [ -n "$JAVA_DEBUG_SUSPEND" ]; then
      options="$options,suspend=y"
    else
      options="$options,suspend=n"
    fi
  fi
  echo "$options"
}

java_options() {
local option_lines=$(cat <<EOF
-Djava.security.egd=file:/dev/./urandom
-XX:-OmitStackTraceInFastThrow
-Dquarkus.http.host=0.0.0.0
-Djava.util.logging.manager=org.jboss.logmanager.LogManager
-XX:+UseContainerSupport
-XX:MaxRAMPercentage=50
EOF
)

  echo "$option_lines" | tr '\n' ' '
}

launch_reaugmentation() {
  local CMD="/usr/bin/java $(java_options) -Dquarkus.launch.rebuild=true -jar /app/quarkus-run.jar"

  echo "$CMD"
  $CMD
  echo "Reaugmentation process ended with $?"
}

launch_main_process() {
  local CMD="/usr/bin/java $(java_options) $(java_debug) -jar /app/quarkus-run.jar"

  echo "$CMD"
  $CMD
  echo "Java process ended with $?"
}

troubleshooting_sleep_if_needed() {
  if [ -n "$SLEEP_BEFORE_EXIT" ]; then
    echo "Sleeping before exit for $SLEEP_BEFORE_EXIT seconds"
    sleep $SLEEP_BEFORE_EXIT
    echo "Done"
  fi
}

create_temp_s3_dir() {
  NEW_DIR=${S3_DOWNLOAD_CACHE_DIR:-output}
  echo "Creating $NEW_DIR"
  mkdir -p "$NEW_DIR"

  if [ $? -ne 0 ]; then
      echo "Error: Failed to create temp s3 directory '$NEW_DIR'" >&2
      exit 1
  fi
}

create_temp_s3_dir
launch_reaugmentation
launch_main_process
troubleshooting_sleep_if_needed
