#!/bin/sh

log() {
  echo "[$(date +%FT%T%Z)][INFO][class=entrypoint.sh] $1"
}

if [ -n "${TLS_CERT_DIR}" ] ; then
  cp /scripts/nginx/nginx-https.conf /etc/nginx/nginx.conf
else
  cp /scripts/nginx/nginx.conf /etc/nginx/nginx.conf
fi

pids=""
if [ -n "${DIAG_HTTP_STORAGE_HOST}" ] ; then
  REPLACEMENT="proxy_pass ${DIAG_HTTP_STORAGE_HOST};"
  log "Enabled forwarding of diagnostic files to ${DIAG_HTTP_STORAGE_HOST}"
elif [ -n "${DIAG_PV_MOUNT_PATH}" ] ; then
  REPLACEMENT="root ${DIAG_PV_MOUNT_PATH};dav_methods  PUT;create_full_put_path on;"
  log "Saving diagnostic files to ${DIAG_PV_MOUNT_PATH}"
  mkdir -p "${DIAG_PV_MOUNT_PATH}"/diagnostic

  sed -i "s^###^^" /etc/nginx/nginx.conf

  /usr/share/docroot/api/v1/diagnostic/tools/prf_dump_writer run > /dev/stdout 2>&1 &
  pids="$pids $!"
else
  REPLACEMENT="return 403 'Diagnostic storage has not been configured. Please specify either DIAG_HTTP_STORAGE_HOST or DIAG_PV_MOUNT_PATH';"
  log "Neither 'DIAG_HTTP_STORAGE_HOST' nor 'DIAG_PV_MOUNT_PATH' have been specified. Diagnostic files will be discarded"
fi

sed -i "s^TO_BE_REPLACED_WITH_DIAGNOSTIC_TARGET_CONFIG^${REPLACEMENT}^" /etc/nginx/nginx.conf

log "Starting Nginx service"
nginx -g "daemon off;" > /dev/stdout 2>&1 &
pids="$pids $!"

# Wait any process
while kill -0 "$pids" 2> /dev/null; do sleep 1; done;
