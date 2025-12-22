# Makefile for diagtool Go application

# Variables
BINARY_NAME=diagtools
BUILD_DIR=build

# Default target
.PHONY: all
all: fmt vet build

# Build the application
.PHONY: fmt build
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 go build -o $(BUILD_DIR)/$(BINARY_NAME) .

# Build for multiple platforms (Linux, Windows, macOS)
.PHONY: build-all
build-all:
	@echo "Building $(BINARY_NAME) for all platforms..."
	@mkdir -p $(BUILD_DIR)
	@# Build Linux binaries using Docker (amd64 and arm64)
	$(call build_platform,amd64,linux)
	$(call build_platform,arm64,linux)
	@# Build Windows binary using Docker (amd64)
	$(call build_platform,amd64,windows)
	@# Build macOS binaries only on macOS (requires CGO and native toolchain)
	@if [ "$$(uname -s)" = "Darwin" ]; then \
		echo "Building darwin-amd64 binary..."; \
		CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 .; \
		echo "Building darwin-arm64 binary..."; \
		CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 .; \
	else \
		echo "Skipping darwin builds (not running on macOS)"; \
	fi
	@echo "All platform binaries built successfully:"
	@ls -lh $(BUILD_DIR)/$(BINARY_NAME)-*

# Internal helper to build for a specific platform using BuildKit
# Args: $(1)=arch, $(2)=os (default: linux)
define build_platform
	@echo "Building $(2)-$(1) binary..."
	@DOCKER_BUILDKIT=1 docker buildx build \
		--target export \
		--build-arg TARGETOS=$(2) \
		--build-arg TARGETARCH=$(1) \
		--output type=local,dest=$(BUILD_DIR)/tmp-$(2)-$(1) \
		--load=false \
		-f Dockerfile .
	@if [ "$(2)" = "windows" ]; then \
		mv $(BUILD_DIR)/tmp-$(2)-$(1)/out/diagtools.exe $(BUILD_DIR)/$(BINARY_NAME)-$(2)-$(1).exe; \
	else \
		mv $(BUILD_DIR)/tmp-$(2)-$(1)/out/diagtools $(BUILD_DIR)/$(BINARY_NAME)-$(2)-$(1); \
	fi
	@rm -rf $(BUILD_DIR)/tmp-$(2)-$(1)
endef

# Build Linux binaries using Docker with musl (works from any platform)
.PHONY: build-linux-docker
build-linux-docker:
	@echo "Building $(BINARY_NAME) for Linux (amd64/arm64) with musl using Docker..."
	@mkdir -p $(BUILD_DIR)
	$(call build_platform,amd64,linux)
	$(call build_platform,arm64,linux)
	@echo "Linux binaries built successfully with musl (static linking)"

# Build only Linux amd64 using Docker with musl (faster for development)
.PHONY: build-linux-amd64-docker
build-linux-amd64-docker:
	@echo "Building $(BINARY_NAME) for Linux amd64 with musl using Docker..."
	@mkdir -p $(BUILD_DIR)
	$(call build_platform,amd64,linux)
	@echo "Linux amd64 binary built successfully with musl (static linking)"

# Build Windows amd64 binary using Docker (cross-compilation with MinGW)
.PHONY: build-windows-docker
build-windows-docker:
	@echo "Building $(BINARY_NAME) for Windows amd64 using Docker..."
	@mkdir -p $(BUILD_DIR)
	$(call build_platform,amd64,windows)
	@echo "Windows amd64 binary built successfully (static linking)"

# Build for specific architecture (usage: make build-arch GOOS=linux GOARCH=amd64)
.PHONY: build-arch
build-arch:
	@echo "Building $(BINARY_NAME) for $(GOOS)/$(GOARCH)..."
	@mkdir -p $(BUILD_DIR)
	@if [ -z "$(GOOS)" ] || [ -z "$(GOARCH)" ]; then \
		echo "Error: GOOS and GOARCH must be specified"; \
		echo "Usage: make build-arch GOOS=linux GOARCH=amd64"; \
		echo "Available GOOS: linux, windows, darwin, freebsd, openbsd, netbsd, plan9, solaris"; \
		echo "Available GOARCH: amd64, 386, arm, arm64, mips, mips64, mips64le, mipsle, ppc64, ppc64le, s390x, wasm"; \
		exit 1; \
	fi
	@if [ "$(GOOS)" = "windows" ]; then \
		CGO_ENABLED=1 go build -o $(BUILD_DIR)/$(BINARY_NAME)-$(GOOS)-$(GOARCH).exe .; \
	else \
		CGO_ENABLED=1 go build -o $(BUILD_DIR)/$(BINARY_NAME)-$(GOOS)-$(GOARCH) .; \
	fi

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)

# Run tests
.PHONY: test
test:
	@echo "Running tests..."
	go test ./...

# Run tests with coverage
.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Vet code
.PHONY: vet
vet:
	@echo "Vetting code..."
	go vet ./...

# Lint code
.PHONY: lint
lint:
	@echo "Linting code..."
	golangci-lint run

# Download dependencies
.PHONY: deps
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Install the binary
.PHONY: install
install: build
	@echo "Installing $(BINARY_NAME)..."
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/

# Run the application (for testing)
.PHONY: run
run: build
	@echo "Running $(BINARY_NAME)..."
	./$(BUILD_DIR)/$(BINARY_NAME) --help

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build                       - Build the application for current platform"
	@echo "  build-all                   - Build for all platforms (Linux, Windows, macOS)"
	@echo ""
	@echo "Docker builds (BuildKit with static linking):"
	@echo "  build-linux-docker          - Build for Linux (amd64 & arm64) with musl (static)"
	@echo "  build-linux-amd64-docker    - Build for Linux amd64 with musl (static, faster)"
	@echo "  build-windows-docker        - Build for Windows amd64 with MinGW (static)"
	@echo ""
	@echo "Other targets:"
	@echo "  build-arch                  - Build for specific architecture (GOOS=linux GOARCH=amd64)"
	@echo "  clean                       - Clean build artifacts"
	@echo "  test                        - Run tests"
	@echo "  test-coverage               - Run tests with coverage report"
	@echo "  fmt                         - Format code"
	@echo "  vet                         - Vet code for potential issues"
	@echo "  lint                        - Lint code (requires golangci-lint)"
	@echo "  deps                        - Download and tidy dependencies"
	@echo "  install                     - Install the binary to /usr/local/bin"
	@echo "  run                         - Build and run the application with --help"
	@echo "  help                        - Show this help message"
