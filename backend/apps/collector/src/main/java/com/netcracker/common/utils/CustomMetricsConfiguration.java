package com.netcracker.common.utils;


import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Arrays;

@Singleton
public class CustomMetricsConfiguration {

    @ConfigProperty(name = "service.persistence", defaultValue="unknown")
    String persistenceType;


    @Produces
    @Singleton
    public MeterFilter configureAllRegistries() {
        return MeterFilter.commonTags(Arrays.asList(Tag.of("db", persistenceType)));
    }

    /** Enable histogram buckets for a specific timer */
    @Produces
    @Singleton
    public MeterFilter enableHistogram() {
        return new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
//                if(id.getName().equals("db.timer")) {
//                    return DistributionStatisticConfig.builder()
//                            .serviceLevelObjectives(0.1f, 0.2f, 0.5f, 1, 2, 5, 10)
//                            .percentilesHistogram(false)
//                            .build()
//                            .merge(config);
//                }

                if(id.getName().startsWith("http.server.requests")) {
                    return DistributionStatisticConfig.builder()
                            .percentiles(0.5, 0.95, 0.99)
                            .percentilesHistogram(false)
                            .build()
                            .merge(config);
                }
                return config;
            }
        };
    }
}
