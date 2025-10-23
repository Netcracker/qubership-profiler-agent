#!/usr/bin/env bash
set -e

export NODE_TLS_REJECT_UNAUTHORIZED=0

echo Used version $CLOUD_PROFILER_UI_VERSION

npm ci
npm run build
npm version --git-tag-version=false --commit-hooks=false -- $CLOUD_PROFILER_UI_VERSION
npm pack
