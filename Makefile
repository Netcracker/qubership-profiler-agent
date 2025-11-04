# Cloud Profiler Root Makefile
# Orchestrates builds for all applications and deployment components

.PHONY: help build-all clean-all test-all docker-build-all archive-all \
	apps tools charts examples delivery \
	apps-build apps-clean apps-test apps-docker apps-archive \
	tools-build tools-clean tools-test tools-docker tools-archive \
	charts-build charts-clean \
	examples-build examples-clean \
	delivery-build delivery-clean

# Variables
APPS_DIR := apps
TOOLS_DIR := tools
CHARTS_DIR := charts
EXAMPLES_DIR := examples
DELIVERY_DIR := delivery

# Build configuration
# Set SKIP_FRONTEND=Y to exclude the frontend (query app) from builds
# Example: SKIP_FRONTEND=Y make build-all
export SKIP_FRONTEND

# Application names (production components)
APPS := collector dumps-collector maintenance query

# Tool names (development/support tools)
TOOLS := data-generator load-generator migration

# Default target
help:
	@echo "Cloud Profiler - Complete Build System"
	@echo "======================================"
	@echo ""
	@echo "Main targets:"
	@echo "  build-all        - Build everything (apps, tools, charts, examples, delivery)"
	@echo "  clean-all        - Clean all build artifacts"
	@echo "  test-all         - Run all tests"
	@echo "  docker-build-all - Build all Docker images"
	@echo "  archive-all      - Create all deployment archives"
	@echo ""
	@echo "Build options:"
	@echo "  SKIP_FRONTEND=Y  - Skip frontend (query app) build"
	@echo "                     Example: SKIP_FRONTEND=Y make build-all"
	@echo ""
	@echo "Component targets:"
	@echo "  apps             - Build all applications"
	@echo "  tools            - Build all tools"
	@echo "  charts           - Build/validate Helm charts"
	@echo "  examples         - Build example applications"
	@echo "  delivery         - Build delivery package"
	@echo ""
	@echo "Individual app targets:"
	@for app in $(APPS); do \
		echo "  $$app-build        - Build $$app application"; \
		echo "  $$app-clean        - Clean $$app build artifacts"; \
		echo "  $$app-test         - Run $$app tests"; \
		echo "  $$app-docker       - Build $$app Docker image"; \
		echo "  $$app-archive      - Create $$app deployment archive"; \
	done
	@echo ""
	@echo "Individual tool targets:"
	@for tool in $(TOOLS); do \
		echo "  $$tool-build        - Build $$tool"; \
		echo "  $$tool-clean        - Clean $$tool build artifacts"; \
		echo "  $$tool-test         - Run $$tool tests"; \
		echo "  $$tool-docker       - Build $$tool Docker image"; \
		echo "  $$tool-archive      - Create $$tool deployment archive"; \
	done
	@echo ""
	@echo "Utility targets:"
	@echo "  help-apps         - Show help for apps build system"
	@echo "  help-tools        - Show help for tools build system"
	@echo "  help-<app>        - Show help for specific app (e.g., help-collector)"
	@echo "  help-<tool>       - Show help for specific tool (e.g., help-migration)"

# Build everything
build-all: apps-build tools-build charts-build examples-build delivery-build
	@echo "==> All components built successfully!"

# Clean everything
clean-all: apps-clean tools-clean charts-clean examples-clean delivery-clean
	@echo "==> All build artifacts cleaned!"

# Run all tests
test-all: apps-test tools-test
	@echo "==> All tests completed!"

# Build all Docker images
docker-build-all: apps-docker tools-docker
	@echo "==> All Docker images built successfully!"

# Create all deployment archives
archive-all: apps-archive tools-archive
	@echo "==> All deployment archives created successfully!"

# =============================================================================
# APPS TARGETS
# =============================================================================

# Build all applications
apps: apps-build

apps-build:
	@echo "==> Building all applications..."
	$(MAKE) -C $(APPS_DIR) build-all

apps-clean:
	@echo "==> Cleaning all application build artifacts..."
	$(MAKE) -C $(APPS_DIR) clean-all

apps-test:
	@echo "==> Running all application tests..."
	$(MAKE) -C $(APPS_DIR) test-all

apps-docker:
	@echo "==> Building all application Docker images..."
	$(MAKE) -C $(APPS_DIR) docker-build-all

apps-archive:
	@echo "==> Creating all application deployment archives..."
	$(MAKE) -C $(APPS_DIR) archive-all

# =============================================================================
# TOOLS TARGETS
# =============================================================================

# Build all tools
tools: tools-build

tools-build:
	@echo "==> Building all tools..."
	$(MAKE) -C $(TOOLS_DIR) build-all

tools-clean:
	@echo "==> Cleaning all tool build artifacts..."
	$(MAKE) -C $(TOOLS_DIR) clean-all

tools-test:
	@echo "==> Running all tool tests..."
	$(MAKE) -C $(TOOLS_DIR) test-all

tools-docker:
	@echo "==> Building all tool Docker images..."
	$(MAKE) -C $(TOOLS_DIR) docker-build-all

tools-archive:
	@echo "==> Creating all tool deployment archives..."
	$(MAKE) -C $(TOOLS_DIR) archive-all

# =============================================================================
# INDIVIDUAL COMPONENT TARGETS
# =============================================================================

# Individual app targets
collector-build:
	@echo "==> Building collector..."
	$(MAKE) -C $(APPS_DIR)/collector build

data-generator-build:
	@echo "==> Building data-generator..."
	$(MAKE) -C $(TOOLS_DIR)/data-generator build

dumps-collector-build:
	@echo "==> Building dumps-collector..."
	$(MAKE) -C $(APPS_DIR)/dumps-collector build

load-generator-build:
	@echo "==> Building load-generator..."
	$(MAKE) -C $(TOOLS_DIR)/load-generator build

maintenance-build:
	@echo "==> Building maintenance..."
	$(MAKE) -C $(APPS_DIR)/maintenance build

migration-build:
	@echo "==> Building migration..."
	$(MAKE) -C $(TOOLS_DIR)/migration build

query-build:
	@echo "==> Building query..."
	$(MAKE) -C $(APPS_DIR)/query build

collector-clean:
	@echo "==> Cleaning collector..."
	$(MAKE) -C $(APPS_DIR)/collector clean

data-generator-clean:
	@echo "==> Cleaning data-generator..."
	$(MAKE) -C $(TOOLS_DIR)/data-generator clean

dumps-collector-clean:
	@echo "==> Cleaning dumps-collector..."
	$(MAKE) -C $(APPS_DIR)/dumps-collector clean

load-generator-clean:
	@echo "==> Cleaning load-generator..."
	$(MAKE) -C $(TOOLS_DIR)/load-generator clean

maintenance-clean:
	@echo "==> Cleaning maintenance..."
	$(MAKE) -C $(APPS_DIR)/maintenance clean

migration-clean:
	@echo "==> Cleaning migration..."
	$(MAKE) -C $(TOOLS_DIR)/migration clean

query-clean:
	@echo "==> Cleaning query..."
	$(MAKE) -C $(APPS_DIR)/query clean

collector-test:
	@echo "==> Testing collector..."
	$(MAKE) -C $(APPS_DIR)/collector test

data-generator-test:
	@echo "==> Testing data-generator..."
	$(MAKE) -C $(TOOLS_DIR)/data-generator test

dumps-collector-test:
	@echo "==> Testing dumps-collector..."
	$(MAKE) -C $(APPS_DIR)/dumps-collector test

load-generator-test:
	@echo "==> Testing load-generator..."
	$(MAKE) -C $(TOOLS_DIR)/load-generator test

maintenance-test:
	@echo "==> Testing maintenance..."
	$(MAKE) -C $(APPS_DIR)/maintenance test

migration-test:
	@echo "==> Testing migration..."
	$(MAKE) -C $(TOOLS_DIR)/migration test

query-test:
	@echo "==> Testing query..."
	$(MAKE) -C $(APPS_DIR)/query test

collector-docker:
	@echo "==> Building Docker image for collector..."
	$(MAKE) -C $(APPS_DIR)/collector docker-build

data-generator-docker:
	@echo "==> Building Docker image for data-generator..."
	$(MAKE) -C $(TOOLS_DIR)/data-generator docker-build

dumps-collector-docker:
	@echo "==> Building Docker image for dumps-collector..."
	$(MAKE) -C $(APPS_DIR)/dumps-collector docker-build

load-generator-docker:
	@echo "==> Building Docker image for load-generator..."
	$(MAKE) -C $(TOOLS_DIR)/load-generator docker-build

maintenance-docker:
	@echo "==> Building Docker image for maintenance..."
	$(MAKE) -C $(APPS_DIR)/maintenance docker-build

migration-docker:
	@echo "==> Building Docker image for migration..."
	$(MAKE) -C $(TOOLS_DIR)/migration docker-build

query-docker:
	@echo "==> Building Docker image for query..."
	$(MAKE) -C $(APPS_DIR)/query docker-build

collector-archive:
	@echo "==> Creating archive for collector..."
	$(MAKE) -C $(APPS_DIR)/collector archive

data-generator-archive:
	@echo "==> Creating archive for data-generator..."
	$(MAKE) -C $(TOOLS_DIR)/data-generator archive

dumps-collector-archive:
	@echo "==> Creating archive for dumps-collector..."
	$(MAKE) -C $(APPS_DIR)/dumps-collector archive

load-generator-archive:
	@echo "==> Creating archive for load-generator..."
	$(MAKE) -C $(TOOLS_DIR)/load-generator archive

maintenance-archive:
	@echo "==> Creating archive for maintenance..."
	$(MAKE) -C $(APPS_DIR)/maintenance archive

migration-archive:
	@echo "==> Creating archive for migration..."
	$(MAKE) -C $(TOOLS_DIR)/migration archive

query-archive:
	@echo "==> Creating archive for query..."
	$(MAKE) -C $(APPS_DIR)/query archive


# =============================================================================
# CHARTS TARGETS
# =============================================================================

# Build/validate Helm charts
charts: charts-build

charts-build:
	@echo "==> Validating Helm charts..."
	@if command -v helm >/dev/null 2>&1; then \
		for chart in $(CHARTS_DIR)/*; do \
			if [ -d "$$chart" ] && [ -f "$$chart/Chart.yaml" ]; then \
				echo "Validating chart: $$(basename $$chart)"; \
				helm lint "$$chart" || exit 1; \
			fi; \
		done; \
		echo "==> All Helm charts validated successfully!"; \
	else \
		echo "Warning: Helm not found, skipping chart validation"; \
	fi

charts-clean:
	@echo "==> Cleaning Helm chart artifacts..."
	@find $(CHARTS_DIR) -name "*.tgz" -delete || true
	@echo "==> Helm chart artifacts cleaned!"

# =============================================================================
# EXAMPLES TARGETS
# =============================================================================

# Build example applications
examples: examples-build

examples-build:
	@echo "==> Building example applications..."
	@if [ -f "$(EXAMPLES_DIR)/build.sh" ]; then \
		chmod +x $(EXAMPLES_DIR)/build.sh; \
		$(EXAMPLES_DIR)/build.sh; \
	else \
		echo "No build script found for examples"; \
	fi
	@echo "==> Example applications built successfully!"

examples-clean:
	@echo "==> Cleaning example build artifacts..."
	@rm -rf $(EXAMPLES_DIR)/target || true
	@echo "==> Example build artifacts cleaned!"

# =============================================================================
# DELIVERY TARGETS
# =============================================================================

# Build delivery package
delivery: delivery-build

delivery-build:
	@echo "==> Building delivery package..."
	@if [ -f "$(DELIVERY_DIR)/build.sh" ]; then \
		chmod +x $(DELIVERY_DIR)/build.sh; \
		$(DELIVERY_DIR)/build.sh; \
	else \
		echo "No build script found for delivery"; \
	fi
	@echo "==> Delivery package built successfully!"

delivery-clean:
	@echo "==> Cleaning delivery artifacts..."
	@rm -rf $(DELIVERY_DIR)/helm || true
	@echo "==> Delivery artifacts cleaned!"

# =============================================================================
# HELP TARGETS
# =============================================================================

# Show help for apps
help-apps:
	@echo "Apps build system help:"
	$(MAKE) -C $(APPS_DIR) help

# Show help for tools
help-tools:
	@echo "Tools build system help:"
	$(MAKE) -C $(TOOLS_DIR) help

# Show help for individual apps
help-collector:
	@echo "Help for collector application:"
	$(MAKE) -C $(APPS_DIR)/collector help

help-data-generator:
	@echo "Help for data-generator tool:"
	$(MAKE) -C $(TOOLS_DIR)/data-generator help

help-dumps-collector:
	@echo "Help for dumps-collector application:"
	$(MAKE) -C $(APPS_DIR)/dumps-collector help

help-load-generator:
	@echo "Help for load-generator tool:"
	$(MAKE) -C $(TOOLS_DIR)/load-generator help

help-maintenance:
	@echo "Help for maintenance application:"
	$(MAKE) -C $(APPS_DIR)/maintenance help

help-migration:
	@echo "Help for migration tool:"
	$(MAKE) -C $(TOOLS_DIR)/migration help

help-query:
	@echo "Help for query application:"
	$(MAKE) -C $(APPS_DIR)/query help

