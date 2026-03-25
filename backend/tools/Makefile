# Cloud Profiler Tools Root Makefile
# Builds all tools in the tools directory

.PHONY: help build-all clean-all test-all docker-build-all archive-all

# Variables
TOOLS := data-generator load-generator migration

# Default target
help:
	@echo "Cloud Profiler Tools Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  build-all        - Build all tools"
	@echo "  clean-all        - Clean all build artifacts"
	@echo "  test-all         - Run tests for all tools"
	@echo "  docker-build-all - Build Docker images for all tools"
	@echo "  archive-all      - Create deployment archives for all tools"
	@echo "  help-<tool>      - Show help for specific tool (e.g., help-migration)"
	@echo ""
	@echo "Individual tool targets:"
	@for tool in $(TOOLS); do \
		echo "  $$tool-build        - Build $$tool"; \
		echo "  $$tool-clean        - Clean $$tool build artifacts"; \
		echo "  $$tool-test         - Run $$tool tests"; \
		echo "  $$tool-docker       - Build $$tool Docker image"; \
		echo "  $$tool-archive      - Create $$tool deployment archive"; \
	done

# Build all tools
build-all:
	@echo "==> Building all tools..."
	@for tool in $(TOOLS); do \
		echo "Building $$tool..."; \
		$(MAKE) -C $$tool build || exit 1; \
	done
	@echo "==> All tools built successfully!"

# Clean all build artifacts
clean-all:
	@echo "==> Cleaning all build artifacts..."
	@for tool in $(TOOLS); do \
		echo "Cleaning $$tool..."; \
		$(MAKE) -C $$tool clean || true; \
	done
	@echo "==> All build artifacts cleaned!"

# Run tests for all tools
test-all:
	@echo "==> Running tests for all tools..."
	@for tool in $(TOOLS); do \
		echo "Testing $$tool..."; \
		$(MAKE) -C $$tool test || exit 1; \
	done
	@echo "==> All tests completed!"

# Build Docker images for all tools
docker-build-all:
	@echo "==> Building Docker images for all tools..."
	@for tool in $(TOOLS); do \
		echo "Building Docker image for $$tool..."; \
		$(MAKE) -C $$tool docker-build || exit 1; \
	done
	@echo "==> All Docker images built successfully!"

# Create deployment archives for all tools
archive-all:
	@echo "==> Creating deployment archives for all tools..."
	@for tool in $(TOOLS); do \
		echo "Creating archive for $$tool..."; \
		$(MAKE) -C $$tool archive || exit 1; \
	done
	@echo "==> All deployment archives created successfully!"

# Individual tool targets
data-generator-build:
	@echo "==> Building data-generator..."
	$(MAKE) -C data-generator build

load-generator-build:
	@echo "==> Building load-generator..."
	$(MAKE) -C load-generator build

migration-build:
	@echo "==> Building migration..."
	$(MAKE) -C migration build

data-generator-clean:
	@echo "==> Cleaning data-generator..."
	$(MAKE) -C data-generator clean

load-generator-clean:
	@echo "==> Cleaning load-generator..."
	$(MAKE) -C load-generator clean

migration-clean:
	@echo "==> Cleaning migration..."
	$(MAKE) -C migration clean

data-generator-test:
	@echo "==> Testing data-generator..."
	$(MAKE) -C data-generator test

load-generator-test:
	@echo "==> Testing load-generator..."
	$(MAKE) -C load-generator test

migration-test:
	@echo "==> Testing migration..."
	$(MAKE) -C migration test

data-generator-docker:
	@echo "==> Building Docker image for data-generator..."
	$(MAKE) -C data-generator docker-build

load-generator-docker:
	@echo "==> Building Docker image for load-generator..."
	$(MAKE) -C load-generator docker-build

migration-docker:
	@echo "==> Building Docker image for migration..."
	$(MAKE) -C migration docker-build

data-generator-archive:
	@echo "==> Creating archive for data-generator..."
	$(MAKE) -C data-generator archive

load-generator-archive:
	@echo "==> Creating archive for load-generator..."
	$(MAKE) -C load-generator archive

migration-archive:
	@echo "==> Creating archive for migration..."
	$(MAKE) -C migration archive

# Help targets for individual tools
help-data-generator:
	@echo "Help for data-generator tool:"
	$(MAKE) -C data-generator help

help-load-generator:
	@echo "Help for load-generator tool:"
	$(MAKE) -C load-generator help

help-migration:
	@echo "Help for migration tool:"
	$(MAKE) -C migration help

