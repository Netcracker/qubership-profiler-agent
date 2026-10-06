# Cloud Profiler Apps Root Makefile
# Builds all applications in the apps directory

.PHONY: help build-all clean-all test-all docker-build-all archive-all

# Variables
APPS := dumps-collector profiler-backend

# Default target
help:
	@echo "Cloud Profiler Apps Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  build-all        - Build all applications"
	@echo "  clean-all        - Clean all build artifacts"
	@echo "  test-all         - Run tests for all applications"
	@echo "  docker-build-all - Build Docker images for all applications"
	@echo "  archive-all      - Create deployment archives for all applications"
	@echo "  help-<app>       - Show help for specific app (e.g., help-dumps-collector)"
	@echo ""
	@echo "Individual app targets:"
	@for app in $(APPS); do \
		echo "  $$app-build        - Build $$app application"; \
		echo "  $$app-clean        - Clean $$app build artifacts"; \
		echo "  $$app-test         - Run $$app tests"; \
		echo "  $$app-docker       - Build $$app Docker image"; \
		echo "  $$app-archive      - Create $$app deployment archive"; \
	done

# Build all applications
build-all:
	@echo "==> Building all applications..."
	@for app in $(APPS); do \
		echo "Building $$app..."; \
		$(MAKE) -C $$app build || exit 1; \
	done
	@echo "==> All applications built successfully!"

# Clean all build artifacts
clean-all:
	@echo "==> Cleaning all build artifacts..."
	@for app in $(APPS); do \
		echo "Cleaning $$app..."; \
		$(MAKE) -C $$app clean || true; \
	done
	@echo "==> All build artifacts cleaned!"

# Run tests for all applications
test-all:
	@echo "==> Running tests for all applications..."
	@for app in $(APPS); do \
		echo "Testing $$app..."; \
		$(MAKE) -C $$app test || exit 1; \
	done
	@echo "==> All tests completed!"

# Build Docker images for all applications
docker-build-all:
	@echo "==> Building Docker images for all applications..."
	@for app in $(APPS); do \
		echo "Building Docker image for $$app..."; \
		$(MAKE) -C $$app docker-build || exit 1; \
	done
	@echo "==> All Docker images built successfully!"

# Create deployment archives for all applications
archive-all:
	@echo "==> Creating deployment archives for all applications..."
	@for app in $(APPS); do \
		echo "Creating archive for $$app..."; \
		$(MAKE) -C $$app archive || exit 1; \
	done
	@echo "==> All deployment archives created successfully!"

# Individual app targets
dumps-collector-build:
	@echo "==> Building dumps-collector..."
	$(MAKE) -C dumps-collector build

profiler-backend-build:
	@echo "==> Building profiler-backend..."
	$(MAKE) -C profiler-backend build

dumps-collector-clean:
	@echo "==> Cleaning dumps-collector..."
	$(MAKE) -C dumps-collector clean

profiler-backend-clean:
	@echo "==> Cleaning profiler-backend..."
	$(MAKE) -C profiler-backend clean

dumps-collector-test:
	@echo "==> Testing dumps-collector..."
	$(MAKE) -C dumps-collector test

profiler-backend-test:
	@echo "==> Testing profiler-backend..."
	$(MAKE) -C profiler-backend test

dumps-collector-docker:
	@echo "==> Building Docker image for dumps-collector..."
	$(MAKE) -C dumps-collector docker-build

profiler-backend-docker:
	@echo "==> Building Docker image for profiler-backend..."
	$(MAKE) -C profiler-backend docker-build

dumps-collector-archive:
	@echo "==> Creating archive for dumps-collector..."
	$(MAKE) -C dumps-collector archive

profiler-backend-archive:
	@echo "==> Creating archive for profiler-backend..."
	$(MAKE) -C profiler-backend archive

# Help targets for individual apps
help-dumps-collector:
	@echo "Help for dumps-collector application:"
	$(MAKE) -C dumps-collector help

help-profiler-backend:
	@echo "Help for profiler-backend application:"
	$(MAKE) -C profiler-backend help
