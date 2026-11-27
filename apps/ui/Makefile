# Profiler UI Makefile
# Thin wrappers over the npm scripts in package.json, so the UI runs without
# remembering them. Every target installs dependencies first when
# package-lock.json is newer than node_modules.

.PHONY: help install dev dev-mock test typecheck build clean

# Where the query binary serves the UI, as in the Dockerfile. `make build
# UI_BASE_PATH=/` builds for the root instead.
UI_BASE_PATH ?= /ui/

help:
	@echo "Profiler UI"
	@echo "==========="
	@echo ""
	@echo "  dev-mock   - Dev server at http://localhost:5173/ui/ with the MSW mock, no backend needed"
	@echo "  dev        - Dev server at http://localhost:5173/ui/, /api/v1 proxied to VITE_QUERY_URL"
	@echo "               (default http://localhost:8080, set in vite.config.ts)"
	@echo "  test       - Run the vitest suite once"
	@echo "  typecheck  - Run tsc without emitting"
	@echo "  build      - Build the bundle into dist/ for the query binary to embed"
	@echo "               (base path UI_BASE_PATH, default $(UI_BASE_PATH))"
	@echo "  install    - Install npm dependencies from package-lock.json"
	@echo "  clean      - Remove node_modules"

# npm writes node_modules/.package-lock.json on every install, so it marks
# when the dependencies were last installed.
node_modules/.package-lock.json: package.json package-lock.json
	npm ci

install: node_modules/.package-lock.json

dev-mock: install
	npm run dev:mock

# make exports VITE_QUERY_URL to npm when it is set on the command line or in
# the environment; vite.config.ts holds the default.
dev: install
	npm run dev

test: install
	npm run test

typecheck: install
	npm run typecheck

# vite empties dist/, including the tracked placeholder that keeps go:embed
# compiling without a bundle, so put it back.
build: install
	UI_BASE_PATH=$(UI_BASE_PATH) npm run build
	touch dist/.gitkeep

clean:
	rm -rf node_modules
