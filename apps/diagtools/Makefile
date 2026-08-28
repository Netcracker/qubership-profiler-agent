# Makefile for diagtool Go application
#
# Cross-compilation is performed via zig cc (https://ziglang.org) — it bundles
# clang + libc headers + linker for all supported targets, so there is no need
# for Docker or platform-specific gcc/mingw toolchains.

# Variables
BINARY_NAME=diagtools
BUILD_DIR=build

# Zig cross-compiler (used as CC/CXX when cross-compiling cgo code)
ZIG ?= zig

# Strip Go symbol table (-s) and DWARF (-w) from release artifacts.
# On Linux/Windows via zig this is critical: zig ships musl/compiler-rt
# with DWARF by default, which would bloat the binary by ~8 MiB. On
# darwin the effect is smaller (~1-2 MiB) but kept for consistency.
# Go panics/stack traces rely on .gopclntab, not DWARF, so stripping
# costs nothing in practice. Omitted from the plain `build` target so
# local dev builds remain debuggable with delve.
STRIP_LDFLAGS := -s -w

# Static linkage via external linker — produces a self-contained binary
# that does not require a matching libc on the target system.
STATIC_LDFLAGS := -ldflags '$(STRIP_LDFLAGS) -linkmode external -extldflags "-static"'

# Default target
.PHONY: all
all: fmt vet build

# Build the application for the current platform (native toolchain)
.PHONY: build
build:
	@echo "Building $(BINARY_NAME) for current platform..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 go build -o $(BUILD_DIR)/$(BINARY_NAME) .

# Cross-compile using `zig cc` as the C compiler.
# Args: $(1)=GOOS, $(2)=GOARCH, $(3)=zig target triple, $(4)=binary suffix (e.g. .exe)
define build_zig
	@echo "Building $(1)/$(2) via zig cc (target: $(3))..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 GOOS=$(1) GOARCH=$(2) \
	  CC="$(ZIG) cc -target $(3)" \
	  CXX="$(ZIG) c++ -target $(3)" \
	  go build $(STATIC_LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-$(1)-$(2)$(4) .
endef

# Verify that the zig toolchain is available before attempting a cross build.
# Without this the failure surfaces deep inside cgo as a confusing
# "exec: <ZIG> cc: executable file not found" from the Go driver.
.PHONY: check-zig
check-zig:
	@command -v $(ZIG) >/dev/null 2>&1 || { \
	  echo "ERROR: '$(ZIG)' not found in PATH."; \
	  echo "Install zig from https://ziglang.org/download/ (tested with 0.15.x),"; \
	  echo "or point make at an existing binary:  make ZIG=/path/to/zig <target>"; \
	  exit 1; }

# Linux and Windows targets via zig (work from any host: macOS / Linux / Windows)
.PHONY: build-linux-amd64
build-linux-amd64: check-zig
	$(call build_zig,linux,amd64,x86_64-linux-musl,)

.PHONY: build-linux-arm64
build-linux-arm64: check-zig
	$(call build_zig,linux,arm64,aarch64-linux-musl,)

.PHONY: build-windows-amd64
build-windows-amd64: check-zig
	$(call build_zig,windows,amd64,x86_64-windows-gnu,.exe)

# Darwin targets — native toolchain (only valid on macOS host).
# Cross-compiling to darwin from Linux/Windows would require Apple SDK.
.PHONY: build-darwin-amd64
build-darwin-amd64:
	@echo "Building darwin/amd64 (native)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 \
	  go build -ldflags '$(STRIP_LDFLAGS)' -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 .

.PHONY: build-darwin-arm64
build-darwin-arm64:
	@echo "Building darwin/arm64 (native)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
	  go build -ldflags '$(STRIP_LDFLAGS)' -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 .

# Cross-platform builds that work from any host
.PHONY: build-cross
build-cross: build-linux-amd64 build-linux-arm64 build-windows-amd64

# Build for every platform the host can produce.
# On macOS adds darwin-amd64/arm64 (native); on other hosts darwin is skipped.
.PHONY: build-all
build-all: build-cross
	@if [ "$$(uname -s)" = "Darwin" ]; then \
	  $(MAKE) build-darwin-amd64 build-darwin-arm64; \
	else \
	  echo "Skipping darwin builds (host is not macOS)"; \
	fi
	@echo "All platform binaries built successfully:"
	@ls -lh $(BUILD_DIR)/$(BINARY_NAME)-*

# Build for specific architecture using the native toolchain
# (usage: make build-arch GOOS=linux GOARCH=amd64)
.PHONY: build-arch
build-arch:
	@echo "Building $(BINARY_NAME) for $(GOOS)/$(GOARCH)..."
	@mkdir -p $(BUILD_DIR)
	@if [ -z "$(GOOS)" ] || [ -z "$(GOARCH)" ]; then \
		echo "Error: GOOS and GOARCH must be specified"; \
		echo "Usage: make build-arch GOOS=linux GOARCH=amd64"; \
		echo "For cross-compilation prefer dedicated zig-based targets"; \
		echo "  build-linux-amd64 / build-linux-arm64 / build-windows-amd64"; \
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
	golangci-lint run --config ../.github/linters/.golangci.yml

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
	@echo "  build                       - Build for current platform (native toolchain)"
	@echo "  build-all                   - Build every platform supported by the host"
	@echo "                                (Linux/Windows always, darwin only on macOS hosts)"
	@echo "  build-cross                 - Linux (amd64/arm64) + Windows amd64 via zig cc"
	@echo ""
	@echo "Per-platform cross builds (zig cc, no Docker required):"
	@echo "  build-linux-amd64           - Linux amd64 (static, musl)"
	@echo "  build-linux-arm64           - Linux arm64 (static, musl)"
	@echo "  build-windows-amd64         - Windows amd64 (static, MinGW ABI)"
	@echo ""
	@echo "Native builds:"
	@echo "  build-darwin-amd64          - macOS amd64 (requires macOS host)"
	@echo "  build-darwin-arm64          - macOS arm64 (requires macOS host)"
	@echo ""
	@echo "Other targets:"
	@echo "  build-arch                  - Build for specific GOOS/GOARCH using native CC"
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
	@echo ""
	@echo "Variables:"
	@echo "  ZIG=<path>                  - Path to zig executable (default: 'zig' from PATH)"
