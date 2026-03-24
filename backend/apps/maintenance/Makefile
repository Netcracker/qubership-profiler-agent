# Cloud Profiler Maintenance Job Makefile
# Go application with Docker build

.PHONY: help build clean docker-build docker-tag archive test run

# Variables
ARTIFACT_NAME := cloud-profiler-maintenance-job
BUILD_DIR := build
OUTPUT_DIR := $(BUILD_DIR)/_output
SAAS_MANIFEST_DIR := deployments
DOCKER_IMAGE := $(ARTIFACT_NAME)
BINARY_NAME := maintenance_job

# Default target
help:
	@echo "Available targets:"
	@echo "  build         - Build the Go application"
	@echo "  clean         - Clean build artifacts"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-tag    - Tag Docker image (requires DOCKER_NAMES variable)"
	@echo "  archive       - Create deployment archive"
	@echo "  test          - Run tests"
	@echo "  run           - Run the application locally"
	@echo "  all           - Build, docker-build, and archive"

# Clean build artifacts
clean:
	@echo "==> Removing old artifacts"
	rm -rf $(BUILD_DIR)
	rm -f $(BINARY_NAME)

# Build Go application
build: clean
	@echo "==> Building Go application..."
	@if command -v go >/dev/null 2>&1; then \
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o $(BINARY_NAME) main.go; \
	else \
		echo "Go not found, skipping Go build. Use docker-build to build with Docker."; \
	fi

# Run tests
test:
	@echo "==> Running tests..."
	@if command -v go >/dev/null 2>&1; then \
		go test ./...; \
	else \
		echo "Go not found, skipping tests."; \
	fi

# Run the application locally
run: build
	@echo "==> Running application..."
	./$(BINARY_NAME)

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
