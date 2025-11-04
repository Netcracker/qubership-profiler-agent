build:
	@if command -v go >/dev/null 2>&1; then \
		go build -o ./bin/parser -ldflags="-w -s" -trimpath github.com/Netcracker/qubership-profiler-backend/tools/load-generator; \
	else \
		echo "Go not found, skipping Go build. Use docker-build to build with Docker."; \
	fi

runservice: build
	@rm -rf ./output/services
	@./bin/parser

runpods: build
	@rm -rf ./output/pods
	@./bin/parser

run: build
	@./bin/parser

sizeservice:
	@ls -lh ./output/services

sizepods:
	@ls -lh ./output/pods

click:
	@~/clickhouse local -q "select * from file('/mnt/c/Git/cdt-load-generator/output/services/namespace0_service0_calls.parquet')" > click_service_output_$(N).txt

clickjson:
	@~/clickhouse local --query "SELECT * FROM file('/mnt/c/Git/cdt-load-generator/output/collectors/collector1_calls_zstd.parquet', ParquetMetadata) FORMAT PrettyJSONEachRow" > calls_for_collector_zstd.json

cli_build:
	@if command -v go >/dev/null 2>&1; then \
		go build -o ./bin/clg -ldflags="-w -s" -trimpath github.com/Netcracker/qubership-profiler-backend/tools/load-generator; \
	else \
		echo "Go not found, skipping Go build. Use docker-build to build with Docker."; \
	fi

cli_run: cli_build
	./bin/clg -namespace 2 -service 2 -pod 2 -calls 1000 -dumps 5 -apptype all -startdate 2024/01/29 -enddate 2024/01/30
