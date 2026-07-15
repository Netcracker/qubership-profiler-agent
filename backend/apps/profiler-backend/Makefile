# Profiler backend (Go) Makefile

.PHONY: help build clean test docker-build archive run

ARTIFACT_NAME := profiler-backend
BINARY_NAME := profiler-backend
DOCKER_IMAGE := $(ARTIFACT_NAME)

# Build settings - can be overridden for cross-compilation
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

help:
	@echo "Available targets:"
	@echo "  build         - Build the Go application"
	@echo "  clean         - Clean build artifacts"
	@echo "  test          - Run tests"
	@echo "  docker-build  - Build Docker image"
	@echo "  archive       - No-op; the Docker image is the deliverable"

clean:
	@echo "==> Removing old artifacts"
	rm -f $(BINARY_NAME)

build:
	@echo "==> Building Go application for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(BINARY_NAME) .

test:
	@echo "==> Running tests..."
	go test ./...

docker-build:
	@echo "==> Building Docker image $(DOCKER_IMAGE)..."
	cd ../.. && docker build -f apps/profiler-backend/Dockerfile -t $(DOCKER_IMAGE) .

archive:
	@echo "==> No archive for $(ARTIFACT_NAME); the Docker image is the deliverable"
