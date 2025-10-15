FROM eclipse-temurin:17-jre

LABEL org.opencontainers.image.title="Qubership Mock Profiler Collector"
LABEL org.opencontainers.image.description="Mock collector server for testing Qubership Profiler Agent"
LABEL org.opencontainers.image.vendor="Qubership"
LABEL org.opencontainers.image.source="https://github.com/Netcracker/qubership-profiler-agent"

# Create app directory
WORKDIR /app

# Copy the mock-collector distribution
COPY build/install/mock-collector /app/

# Create logs directory
RUN mkdir -p /app/logs && \
    chmod 755 /app/bin/mock-collector

# Expose the default profiler port
EXPOSE 1715

# Run the mock collector
ENTRYPOINT ["/app/bin/mock-collector"]
CMD []
