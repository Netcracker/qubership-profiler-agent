# Makefile for diagtool Go application

# Variables
BINARY_NAME=diagtools
BUILD_DIR=target

# Default target
.PHONY: all
all: fmt vet build

# Build the application
.PHONY: fmt build
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) .

# Build for multiple platforms
.PHONY: build-all
build-all:
	@echo "Building $(BINARY_NAME) for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 .
	GOOS=windows GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe .
	GOOS=darwin GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 .

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
		go build -o $(BUILD_DIR)/$(BINARY_NAME)-$(GOOS)-$(GOARCH).exe .; \
	else \
		go build -o $(BUILD_DIR)/$(BINARY_NAME)-$(GOOS)-$(GOARCH) .; \
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
	@echo "  build        - Build the application"
	@echo "  build-all    - Build for multiple platforms (Linux, Windows, macOS)"
	@echo "  build-arch   - Build for specific architecture (GOOS=linux GOARCH=amd64)"
	@echo "  clean        - Clean build artifacts"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage report"
	@echo "  fmt          - Format code"
	@echo "  vet          - Vet code for potential issues"
	@echo "  lint         - Lint code (requires golangci-lint)"
	@echo "  deps         - Download and tidy dependencies"
	@echo "  install      - Install the binary to /usr/local/bin"
	@echo "  run          - Build and run the application with --help"
	@echo "  help         - Show this help message"
