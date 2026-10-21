# profiler CLI Makefile
# A command-line client for agents and people; it ships as a binary, not an image.

.PHONY: help build clean test install docker-build archive e2e

BUILD_DIR := build
BINARY_NAME := profiler

help:
	@echo "Available targets:"
	@echo "  build         - Build the profiler binary for this machine into $(BUILD_DIR)/"
	@echo "  install       - Install the profiler binary into GOBIN"
	@echo "  clean         - Clean build artifacts"
	@echo "  test          - Run tests"
	@echo "  e2e           - Start the dev stack, seed it with ui-seed, run the CLI end to end, stop it"
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

# The request-investigation scenario against the docker compose dev stack
# (agent-access plan task 1.7). The stack is rebuilt from this checkout.
REPO_ROOT := ../..
COMPOSE_DOWN := cd $(REPO_ROOT) && docker compose down -v --remove-orphans
# The readiness probes the smoke tests poll. The collector opens its agent
# listener only after S3 setup and hot-store recovery, and ui-seed dials it
# once, so seeding waits for READY.
COLLECTOR_READY_URL := http://localhost:8081/internal/v1/health/ready
QUERY_READY_URL := http://localhost:8080/api/v1/health/ready
READY_TIMEOUT_SECONDS := 120

# One shell runs the whole lifecycle: the EXIT trap stops the stack whether
# startup, seeding, or the test fails, and the shell keeps the failing status.
e2e:
	$(COMPOSE_DOWN)
	@trap '$(COMPOSE_DOWN)' EXIT; set -e; \
	(cd $(REPO_ROOT) && docker compose up --build -d); \
	for url in $(COLLECTOR_READY_URL) $(QUERY_READY_URL); do \
		echo "==> Waiting for $$url"; \
		deadline=$$(($$(date +%s) + $(READY_TIMEOUT_SECONDS))); \
		until curl -fs -o /dev/null "$$url"; do \
			if [ "$$(date +%s)" -ge "$$deadline" ]; then \
				echo "$$url is not READY after $(READY_TIMEOUT_SECONDS)s" >&2; exit 1; \
			fi; \
			sleep 1; \
		done; \
	done; \
	(cd $(REPO_ROOT) && go run ./tools/ui-seed); \
	PROFILER_URL=http://localhost:8080 go test -tags e2e -count=1 -run E2E -v ./cmd/
