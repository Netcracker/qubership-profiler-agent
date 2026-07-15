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
# collector and query are excluded for now
APPS := dumps-collector maintenance profiler-backend

# Tool names (development/support tools)
TOOLS := load-generator migration

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

profiler-backend-build:
	@echo "==> Building profiler-backend..."
	$(MAKE) -C $(APPS_DIR)/profiler-backend build

query-build:
	@echo "==> Building query..."
	$(MAKE) -C $(APPS_DIR)/query build

collector-clean:
	@echo "==> Cleaning collector..."
	$(MAKE) -C $(APPS_DIR)/collector clean

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

profiler-backend-clean:
	@echo "==> Cleaning profiler-backend..."
	$(MAKE) -C $(APPS_DIR)/profiler-backend clean

query-clean:
	@echo "==> Cleaning query..."
	$(MAKE) -C $(APPS_DIR)/query clean

collector-test:
	@echo "==> Testing collector..."
	$(MAKE) -C $(APPS_DIR)/collector test

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

profiler-backend-test:
	@echo "==> Testing profiler-backend..."
	$(MAKE) -C $(APPS_DIR)/profiler-backend test

query-test:
	@echo "==> Testing query..."
	$(MAKE) -C $(APPS_DIR)/query test

collector-docker:
	@echo "==> Building Docker image for collector..."
	$(MAKE) -C $(APPS_DIR)/collector docker-build

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

profiler-backend-docker:
	@echo "==> Building Docker image for profiler-backend..."
	$(MAKE) -C $(APPS_DIR)/profiler-backend docker-build

query-docker:
	@echo "==> Building Docker image for query..."
	$(MAKE) -C $(APPS_DIR)/query docker-build

collector-archive:
	@echo "==> Creating archive for collector..."
	$(MAKE) -C $(APPS_DIR)/collector archive

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

profiler-backend-archive:
	@echo "==> Creating archive for profiler-backend..."
	$(MAKE) -C $(APPS_DIR)/profiler-backend archive

query-archive:
	@echo "==> Creating archive for query..."
	$(MAKE) -C $(APPS_DIR)/query archive


# =============================================================================
# SMOKE TEST (docker-compose stack: collector + query + MinIO)
# =============================================================================

.PHONY: smoke
smoke:
	@echo "==> Starting the dev stack..."
	docker compose down -v --remove-orphans
	docker compose up --build -d
	@echo "==> Running the end-to-end smoke test..."
	@status=0; go test -tags smoke -count=1 -timeout 20m -v ./libs/tests/smoke/... || status=$$?; \
	docker compose down -v --remove-orphans; \
	exit $$status

# Real-agent E2E: drives the ACTUAL Java profiler agent (built from this repo,
# needs a JDK + Gradle) against the same dev stack and asserts the adversarial
# method/param strings round-trip byte-exact. This is the acceptance gate for
# the decoder fixes and currently FAILS on two known bugs (readChar signedness
# and the empty dictionary word); see libs/tests/smoke_realagent and
# ../scripts/e2e-realagent/README.md.
.PHONY: smoke-realagent
smoke-realagent:
	@echo "==> Starting the dev stack..."
	docker compose down -v --remove-orphans
	docker compose up --build -d
	@echo "==> Running the real-agent E2E test (builds the Java agent)..."
	@status=0; go test -tags smoke_realagent -count=1 -timeout 20m -v ./libs/tests/smoke_realagent/... || status=$$?; \
	docker compose down -v --remove-orphans; \
	exit $$status

# Real-agent v3.1.3 E2E: the regression gate for the "gc" stream compatibility
# fix. Downloads the ACTUAL v3.1.3 Java agent release from Maven Central
# (v3.1.4 removed the "gc" stream this test exercises, so HEAD can't
# reproduce it) against the same dev stack and asserts its calls land in
# /api/v1/calls; see libs/tests/smoke_realagent/realagent_v313_test.go and
# ../scripts/e2e-realagent/README.md. Kept separate from smoke-realagent: it
# fetches a released old version instead of building HEAD.
.PHONY: smoke-realagent-v313
smoke-realagent-v313:
	@echo "==> Starting the dev stack..."
	docker compose down -v --remove-orphans
	docker compose up --build -d
	@echo "==> Running the real-agent v3.1.3 E2E test (fetches the v3.1.3 Java agent from Maven Central)..."
	@status=0; go test -tags smoke_realagent_v313 -count=1 -timeout 20m -v ./libs/tests/smoke_realagent/... || status=$$?; \
	docker compose down -v --remove-orphans; \
	exit $$status

# Pure-Go adversarial decoder gate: the no-JDK, no-docker counterpart of
# smoke-realagent. The wire helpers now emit faithful UTF-16, so they surface
# the same two decoder bugs (readChar signedness and the empty dictionary word)
# without building the Java agent or bringing the stack up. Like smoke-realagent
# it FAILS today; the failing assertions are the deliverable.
.PHONY: adversarial
adversarial:
	@echo "==> Running the pure-Go adversarial decoder gate..."
	go test -tags adversarial -count=1 -v ./libs/tests/adversarial/...

# In-cluster smoke on kind (CI target). For OrbStack localdev:
#   make kind-smoke KIND_CONTEXT=orbstack KIND_SKIP_LOAD=1
.PHONY: kind-smoke
kind-smoke:
	deploy/kind-smoke.sh

# helm template (monitoring objects ON, so the CRD manifests render too)
# validated by kubeconform: a local binary when present, docker otherwise;
# CRD schemas come from the datree catalog.
.PHONY: helm-lint
helm-lint:
	@echo "==> helm lint charts/profiler-backend..."
	helm lint charts/profiler-backend
	@echo "==> Rendering with monitoring enabled and validating with kubeconform..."
	@rm -rf .helm-render
	helm template profiler-backend charts/profiler-backend \
		-f deploy/values-kind.yaml \
		--set metrics.serviceMonitor.enabled=true \
		--set metrics.prometheusRule.enabled=true \
		--output-dir .helm-render
	@if command -v kubeconform >/dev/null 2>&1; then \
		kubeconform -strict -summary \
			-schema-location default \
			-schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
			.helm-render; \
	else \
		docker run --rm -v "$(CURDIR)/.helm-render:/manifests:ro" ghcr.io/yannh/kubeconform:latest \
			-strict -summary \
			-schema-location default \
			-schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
			/manifests; \
	fi
	@rm -rf .helm-render

# promtool unit tests over the alert rules the PrometheusRule ships.
.PHONY: rules-test
rules-test:
	@if command -v promtool >/dev/null 2>&1; then \
		promtool test rules charts/profiler-backend/tests/prometheus/rules_test.yaml; \
	else \
		docker run --rm -v "$(CURDIR)/charts/profiler-backend:/chart:ro" \
			--entrypoint promtool prom/prometheus:latest \
			test rules /chart/tests/prometheus/rules_test.yaml; \
	fi

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

help-profiler-backend:
	@echo "Help for profiler-backend application:"
	$(MAKE) -C $(APPS_DIR)/profiler-backend help

help-query:
	@echo "Help for query application:"
	$(MAKE) -C $(APPS_DIR)/query help

