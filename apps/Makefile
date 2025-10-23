# Cloud Profiler Apps Root Makefile
# Builds all applications in the apps directory

.PHONY: help build-all clean-all test-all docker-build-all archive-all

# Variables
APPS := collector data-generator dumps-collector load-generator maintenance migration query

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
	@echo "  help-<app>       - Show help for specific app (e.g., help-collector)"
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
collector-build:
	@echo "==> Building collector..."
	$(MAKE) -C collector build

compactor-build:
	@echo "==> Building compactor..."
	$(MAKE) -C compactor build

data-generator-build:
	@echo "==> Building data-generator..."
	$(MAKE) -C data-generator build

dumps-collector-build:
	@echo "==> Building dumps-collector..."
	$(MAKE) -C dumps-collector build

load-generator-build:
	@echo "==> Building load-generator..."
	$(MAKE) -C load-generator build

maintenance-build:
	@echo "==> Building maintenance..."
	$(MAKE) -C maintenance build

migration-build:
	@echo "==> Building migration..."
	$(MAKE) -C migration build

query-build:
	@echo "==> Building query..."
	$(MAKE) -C query build

collector-clean:
	@echo "==> Cleaning collector..."
	$(MAKE) -C collector clean

compactor-clean:
	@echo "==> Cleaning compactor..."
	$(MAKE) -C compactor clean

data-generator-clean:
	@echo "==> Cleaning data-generator..."
	$(MAKE) -C data-generator clean

dumps-collector-clean:
	@echo "==> Cleaning dumps-collector..."
	$(MAKE) -C dumps-collector clean

load-generator-clean:
	@echo "==> Cleaning load-generator..."
	$(MAKE) -C load-generator clean

maintenance-clean:
	@echo "==> Cleaning maintenance..."
	$(MAKE) -C maintenance clean

migration-clean:
	@echo "==> Cleaning migration..."
	$(MAKE) -C migration clean

query-clean:
	@echo "==> Cleaning query..."
	$(MAKE) -C query clean

collector-test:
	@echo "==> Testing collector..."
	$(MAKE) -C collector test

compactor-test:
	@echo "==> Testing compactor..."
	$(MAKE) -C compactor test

data-generator-test:
	@echo "==> Testing data-generator..."
	$(MAKE) -C data-generator test

dumps-collector-test:
	@echo "==> Testing dumps-collector..."
	$(MAKE) -C dumps-collector test

load-generator-test:
	@echo "==> Testing load-generator..."
	$(MAKE) -C load-generator test

maintenance-test:
	@echo "==> Testing maintenance..."
	$(MAKE) -C maintenance test

migration-test:
	@echo "==> Testing migration..."
	$(MAKE) -C migration test

query-test:
	@echo "==> Testing query..."
	$(MAKE) -C query test

collector-docker:
	@echo "==> Building Docker image for collector..."
	$(MAKE) -C collector docker-build

compactor-docker:
	@echo "==> Building Docker image for compactor..."
	$(MAKE) -C compactor docker-build

data-generator-docker:
	@echo "==> Building Docker image for data-generator..."
	$(MAKE) -C data-generator docker-build

dumps-collector-docker:
	@echo "==> Building Docker image for dumps-collector..."
	$(MAKE) -C dumps-collector docker-build

load-generator-docker:
	@echo "==> Building Docker image for load-generator..."
	$(MAKE) -C load-generator docker-build

maintenance-docker:
	@echo "==> Building Docker image for maintenance..."
	$(MAKE) -C maintenance docker-build

migration-docker:
	@echo "==> Building Docker image for migration..."
	$(MAKE) -C migration docker-build

query-docker:
	@echo "==> Building Docker image for query..."
	$(MAKE) -C query docker-build

collector-archive:
	@echo "==> Creating archive for collector..."
	$(MAKE) -C collector archive

compactor-archive:
	@echo "==> Creating archive for compactor..."
	$(MAKE) -C compactor archive

data-generator-archive:
	@echo "==> Creating archive for data-generator..."
	$(MAKE) -C data-generator archive

dumps-collector-archive:
	@echo "==> Creating archive for dumps-collector..."
	$(MAKE) -C dumps-collector archive

load-generator-archive:
	@echo "==> Creating archive for load-generator..."
	$(MAKE) -C load-generator archive

maintenance-archive:
	@echo "==> Creating archive for maintenance..."
	$(MAKE) -C maintenance archive

migration-archive:
	@echo "==> Creating archive for migration..."
	$(MAKE) -C migration archive

query-archive:
	@echo "==> Creating archive for query..."
	$(MAKE) -C query archive

# Help targets for individual apps
help-collector:
	@echo "Help for collector application:"
	$(MAKE) -C collector help

help-compactor:
	@echo "Help for compactor application:"
	$(MAKE) -C compactor help

help-data-generator:
	@echo "Help for data-generator application:"
	$(MAKE) -C data-generator help

help-dumps-collector:
	@echo "Help for dumps-collector application:"
	$(MAKE) -C dumps-collector help

help-load-generator:
	@echo "Help for load-generator application:"
	$(MAKE) -C load-generator help

help-maintenance:
	@echo "Help for maintenance application:"
	$(MAKE) -C maintenance help

help-migration:
	@echo "Help for migration application:"
	$(MAKE) -C migration help

help-query:
	@echo "Help for query application:"
	$(MAKE) -C query help
