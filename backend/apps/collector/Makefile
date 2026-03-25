# Cloud Profiler Collector Makefile
# Java/Maven application with Docker build

.PHONY: help build clean docker-build docker-tag archive test

# Variables
ARTIFACT_NAME := cloud-profiler
BUILD_DIR := build
OUTPUT_DIR := $(BUILD_DIR)/_output
SAAS_MANIFEST_DIR := deployments
DOCKER_IMAGE := $(ARTIFACT_NAME)
JAVA_HOME ?= $(shell echo $$JAVA_HOME)

# Default target
help:
	@echo "Available targets:"
	@echo "  build         - Build the Java application"
	@echo "  clean         - Clean build artifacts"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-tag    - Tag Docker image (requires DOCKER_NAMES variable)"
	@echo "  archive       - Create deployment archive"
	@echo "  test          - Run tests"
	@echo "  all           - Build, docker-build, and archive"

# Clean build artifacts
clean:
	@echo "==> Removing old artifacts"
	rm -rf $(BUILD_DIR)
	@if [ -f "./mvnw" ]; then \
		chmod +x ./mvnw; \
		export JAVA_HOME=$(JAVA_HOME) && ./mvnw --batch-mode clean; \
	else \
		echo "Maven wrapper not found, skipping clean"; \
	fi

# Build Java application
build: clean
	@echo "==> Building Java application..."
	@if [ -f "./mvnw" ]; then \
		chmod +x ./mvnw; \
		export JAVA_HOME=$(JAVA_HOME) && ./mvnw --batch-mode clean package; \
	else \
		echo "Maven wrapper not found, using system mvn"; \
		export JAVA_HOME=$(JAVA_HOME) && mvn clean package; \
	fi

# Run tests
test:
	@echo "==> Running tests..."
	@if [ -f "./mvnw" ]; then \
		chmod +x ./mvnw; \
		export JAVA_HOME=$(JAVA_HOME) && ./mvnw --batch-mode test; \
	else \
		echo "Maven wrapper not found, using system mvn"; \
		export JAVA_HOME=$(JAVA_HOME) && mvn test; \
	fi

# Setup build directories
setup:
	@echo "==> Setting up build directories"
	mkdir -p $(OUTPUT_DIR)
	mkdir -p $(SAAS_MANIFEST_DIR)
	cp -R ../charts $(SAAS_MANIFEST_DIR)/

# Build Docker image
docker-build: setup
	@echo "==> Building Docker image..."
	docker build \
		--pull \
		-t $(DOCKER_IMAGE) \
		--build-arg CP_UI_VERSION=$$(cat profiler-ui-version) \
		--no-cache \
		.

# Tag Docker image (requires DOCKER_NAMES environment variable)
docker-tag:
	@if [ -z "$(DOCKER_NAMES)" ]; then \
		echo "Error: DOCKER_NAMES environment variable not set"; \
		exit 1; \
	fi
	@echo "==> Tagging Docker image..."
	@for id in $(DOCKER_NAMES); do \
		docker tag $(DOCKER_IMAGE) "$$id"; \
	done

# Create deployment archive
archive: docker-build
	@echo "==> Archiving artifacts..."
	zip -x build.sh \
		-x description.yaml \
		-x "\*.git\*" \
		-r "$(OUTPUT_DIR)/$(ARTIFACT_NAME).zip" \
		$(SAAS_MANIFEST_DIR)/*

# Build everything
all: build docker-build archive
