# profiler CLI Makefile
# A command-line client for agents and people; it ships as a binary, not an image.

.PHONY: help build clean test install docker-build archive

BUILD_DIR := build
BINARY_NAME := profiler

help:
	@echo "Available targets:"
	@echo "  build         - Build the profiler binary for this machine into $(BUILD_DIR)/"
	@echo "  install       - Install the profiler binary into GOBIN"
	@echo "  clean         - Clean build artifacts"
	@echo "  test          - Run tests"
	@echo "  docker-build  - No-op: the CLI has no image"
	@echo "  archive       - No-op: the CLI has no deployment archive"

clean:
	@echo "==> Removing old artifacts"
	rm -rf $(BUILD_DIR)

build: clean
	@echo "==> Building $(BINARY_NAME)..."
	CGO_ENABLED=0 go build -o $(BUILD_DIR)/$(BINARY_NAME) .

install:
	@echo "==> Installing $(BINARY_NAME)..."
	CGO_ENABLED=0 go build -o $$(go env GOPATH)/bin/$(BINARY_NAME) .

test:
	@echo "==> Running tests..."
	go test ./...

docker-build:
	@echo "==> profiler-cli has no Docker image; skipping"

archive:
	@echo "==> profiler-cli has no deployment archive; skipping"
