# Profiler UI Makefile
# Thin wrappers over the npm scripts in package.json, so the UI runs without
# remembering them. Every target installs dependencies first when
# package-lock.json is newer than node_modules.

.PHONY: help install dev dev-mock test typecheck build clean

# Where `make dev` proxies /api/v1 (vite.config.ts).
VITE_QUERY_URL ?= http://localhost:8080

help:
	@echo "Profiler UI"
	@echo "==========="
	@echo ""
	@echo "  dev-mock   - Dev server at http://localhost:5173/ui/ with the MSW mock, no backend needed"
	@echo "  dev        - Dev server at http://localhost:5173/ui/, /api/v1 proxied to VITE_QUERY_URL"
	@echo "               (default $(VITE_QUERY_URL))"
	@echo "  test       - Run the vitest suite once"
	@echo "  typecheck  - Run tsc without emitting"
	@echo "  build      - Build the bundle into dist/ for the query binary to embed"
	@echo "  install    - Install npm dependencies from package-lock.json"
	@echo "  clean      - Remove node_modules"

# npm writes node_modules/.package-lock.json on every install, so it marks
# when the dependencies were last installed.
node_modules/.package-lock.json: package.json package-lock.json
	npm ci

install: node_modules/.package-lock.json

dev-mock: install
	npm run dev:mock

dev: install
	VITE_QUERY_URL=$(VITE_QUERY_URL) npm run dev

test: install
	npm run test

typecheck: install
	npm run typecheck

# The base path must match where the query binary serves the UI (README, Deployment).
# vite empties dist/, including the tracked placeholder that keeps go:embed
# compiling without a bundle, so put it back.
build: install
	UI_BASE_PATH=/ui/ npm run build
	touch dist/.gitkeep

clean:
	rm -rf node_modules
