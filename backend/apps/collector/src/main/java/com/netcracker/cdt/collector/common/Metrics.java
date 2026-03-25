package com.netcracker.cdt.collector.common;

import io.micrometer.core.instrument.*;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.netcracker.cdt.collector.common.MetricsConst.*;

@LookupIfProperty(name = "service.type", stringValue = "collector")
@ApplicationScoped
public class Metrics {

//    private final static Timer INIT_STREAM_TIMER = Timer
//            .builder(COMMAND_INIT_STREAM_EXECUTION_TIME)
//            .description("Information about command INIT_STREAM execution. For last minute")
//            .distributionStatisticExpiry(Duration.ofMinutes(5))
//            .publishPercentiles(0.5, 0.9, 0.99)
//            .register(io.micrometer.core.instrument.Metrics.globalRegistry);
//    private final static Timer RECEIVE_DATA_TIMER = Timer
//            .builder(COMMAND_RCV_DATA_EXECUTION_TIME)
//            .description("Information about COMMAND_RCV_DATA execution. For last minute")
//            .distributionStatisticExpiry(Duration.ofMinutes(5))
//            .publishPercentiles(0.5, 0.9, 0.99)
//            .register(io.micrometer.core.instrument.Metrics.globalRegistry);

    private final Map<String, DistributionSummary> RECEIVED_BYTES_PER_NS = new ConcurrentHashMap<>();

    private final DistributionSummary RECEIVED_BYTES_DISTRIBUTION = DistributionSummary
            .builder(RECEIVED_BYTES)
            .description("Number of bytes transferred from the agent")
            .distributionStatisticExpiry(Duration.ofMinutes(1))
            .register(io.micrometer.core.instrument.Metrics.globalRegistry);

//    private static final Counter CLEANUP_COUNTER = Counter
//            .builder(CLEANUP_COUNT)
//            .description("Number of cleanups performed")
//            .register(io.micrometer.core.instrument.Metrics.globalRegistry);
//    private static final DistributionSummary CLEANUP_REPORT_CLEARED_BYTES_DS = DistributionSummary
//            .builder(CLEANUP_REPORT_CLEARED_BYTES)
//            .description("Cleared bytes after cleanup (minutely)")
//            .distributionStatisticExpiry(Duration.ofMinutes(1))
//            .register(io.micrometer.core.instrument.Metrics.globalRegistry);
//    private static final Timer CLEANUP_REPORT_EXECUTION_TIME_DS = Timer
//            .builder(CLEANUP_REPORT_EXECUTION_TIME)
//            .description("Time spent on a cleanup (minutely)")
//            .distributionStatisticExpiry(Duration.ofMinutes(1))
//            .register(io.micrometer.core.instrument.Metrics.globalRegistry);
//    private static final Map<String, DistributionSummary> CLEANED_BYTES_PER_NS = new ConcurrentHashMap<>();

    @ConfigProperty(name = "cdt.version", defaultValue="0.0.1")
    private String version;

    @Inject
    MeterRegistry registry;

    @PostConstruct
    private void initialize() {
        Gauge
                .builder(BUILD_INFO, () -> 1)
                .description("Common information about build")
                .tag("version", version)
                .register(registry);
    }

//    public static void setCommandInitStreamExecutionTime(String namespace, String microservice, String pod, long time) {
//        INIT_STREAM_TIMER.record(time, TimeUnit.NANOSECONDS);
//    }
//
//    public static void setCommandRcvDataExecutionTime(String namespace, String microservice, String pod, long time) {
//        RECEIVE_DATA_TIMER.record(time, TimeUnit.NANOSECONDS);
//    }

    public static final int MAX_CONNECTIONS = 2000;

    public void setReceiveFromAgentBytes(String namespace, String microservice, String pod, int bytes) {
        RECEIVED_BYTES_DISTRIBUTION.record(bytes);
        RECEIVED_BYTES_PER_NS.computeIfAbsent(namespace, (ns) -> DistributionSummary
                .builder(RECEIVED_FROM_AGENT_BYTES)
                .description("Number of bytes transferred from the agent in namespace")
                .tag("namespace", ns)
                .distributionStatisticExpiry(Duration.ofMinutes(1))
                .distributionStatisticBufferLength(MAX_CONNECTIONS) // much larger than whatever agents can throw at us
                .register(io.micrometer.core.instrument.Metrics.globalRegistry)
        ).record(bytes);
    }

    public static void setAgentUptime(String namespace, String microservice, String pod, AtomicLong agentUptime) {

    }

}
