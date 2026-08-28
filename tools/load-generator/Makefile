# Build targets for the k6-based load generator. The k6 binary embeds the
# k6/x/cdt module (pkg/cdt) and the go-prometheus-exporter module (go-metrics)
# via tools/load-generator/k6runner; the deliverable is the Docker image, so
# treat the local binary as a development convenience only.

# OrbStack nodes are arm64; override for other stands, e.g.
#   make image PLATFORM=linux/amd64
PLATFORM ?= linux/arm64
IMAGE    ?= cdt-load-generator:dev

# Build from the module root (backend/): the Dockerfile needs go.mod and the
# libs/ packages the k6 module imports.
image:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) \
		-f Dockerfile ../..

build:
	go build -o ./bin/k6 ./k6runner

test:
	go test ./... ../../libs/emulator/...

.PHONY: image build test
