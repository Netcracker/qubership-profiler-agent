# Cloud Profiler Query UI Makefile
# Node.js/TypeScript application

.PHONY: help build clean install test test-coverage lint lint-fix dev start serve archive

# Variables
ARTIFACT_NAME := cloud-profiler-ui
BUILD_DIR := build
OUTPUT_DIR := $(BUILD_DIR)/_output
SAAS_MANIFEST_DIR := deployments
NODE_ENV := production

# Default target
help:
	@echo "Available targets:"
	@echo "  install       - Install dependencies"
	@echo "  build         - Build the application"
	@echo "  clean         - Clean build artifacts"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage"
	@echo "  lint          - Run linting"
	@echo "  lint-fix      - Fix linting issues"
	@echo "  dev           - Start development server"
	@echo "  start         - Start production server"
	@echo "  serve         - Build and serve locally"
	@echo "  archive       - Create deployment archive"
	@echo "  all           - Install, build, and archive"

# Clean build artifacts
clean:
	@echo "==> Removing old artifacts"
	rm -rf $(BUILD_DIR)
	rm -rf build-static
	rm -rf node_modules
	rm -f *.tgz

# Install dependencies
install:
	@echo "==> Installing dependencies..."
	@if command -v npm >/dev/null 2>&1; then \
		if [ -f package-lock.json ]; then \
			export NODE_TLS_REJECT_UNAUTHORIZED=0 && npm ci || echo "Dependency installation failed (likely private packages), skipping..."; \
		else \
			export NODE_TLS_REJECT_UNAUTHORIZED=0 && npm install || echo "Dependency installation failed (likely private packages), skipping..."; \
		fi; \
	else \
		echo "npm not found, skipping dependency installation."; \
	fi

# Build the application
build: install
	@echo "==> Building application..."
	@if command -v npm >/dev/null 2>&1; then \
		export NODE_TLS_REJECT_UNAUTHORIZED=0 && \
		export CLOUD_PROFILER_UI_VERSION=$${CLOUD_PROFILER_UI_VERSION:-0.1.0} && \
		npm run build; \
	else \
		echo "npm not found, skipping build. Use docker-build to build with Docker."; \
	fi

# Run tests
test:
	@echo "==> Running tests..."
	@if command -v npm >/dev/null 2>&1; then \
		npm test; \
	else \
		echo "npm not found, skipping tests."; \
	fi

# Run tests with coverage
test-coverage:
	@echo "==> Running tests with coverage..."
	npm run test:coverage

# Run linting
lint:
	@echo "==> Running linting..."
	npm run lint

# Fix linting issues
lint-fix:
	@echo "==> Fixing linting issues..."
	npm run lint:fix

# Start development server
dev:
	@echo "==> Starting development server..."
	export NODE_TLS_REJECT_UNAUTHORIZED=0 && npm run dev

# Start production server
start:
	@echo "==> Starting production server..."
	export NODE_TLS_REJECT_UNAUTHORIZED=0 && npm start

# Build and serve locally
serve:
	@echo "==> Building and serving locally..."
	export NODE_TLS_REJECT_UNAUTHORIZED=0 && npm run serve

# Setup build directories
setup:
	@echo "==> Setting up build directories"
	mkdir -p $(OUTPUT_DIR)
	mkdir -p $(SAAS_MANIFEST_DIR)
	cp -R ../charts $(SAAS_MANIFEST_DIR)/

# Create npm package
pack: build
	@echo "==> Creating npm package..."
	export NODE_TLS_REJECT_UNAUTHORIZED=0 && \
	export CLOUD_PROFILER_UI_VERSION=$${CLOUD_PROFILER_UI_VERSION:-0.1.0} && \
	npm version --git-tag-version=false --commit-hooks=false -- $$CLOUD_PROFILER_UI_VERSION && \
	npm pack

# Create deployment archive
archive: pack setup
	@echo "==> Archiving artifacts..."
	zip -x build.sh \
		-x description.yaml \
		-x "\*.git\*" \
		-r "$(OUTPUT_DIR)/$(ARTIFACT_NAME).zip" \
		$(SAAS_MANIFEST_DIR)/*

# Build everything
all: build archive
