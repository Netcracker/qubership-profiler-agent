#!/bin/sh

cd /xk6
pwd
ls -la
echo "Wait 10s before test"

sleep 10

echo "Run with $PODS virtual users"
echo "Duration of test: $DURATION"

./k6 run ${K6_PROMETHEUS_RW_SERVER_URL:+-o experimental-prometheus-rw} --summary-mode=full scripts/scenario.js

echo "Stop executing, wait for 3m for scale down"
sleep 180