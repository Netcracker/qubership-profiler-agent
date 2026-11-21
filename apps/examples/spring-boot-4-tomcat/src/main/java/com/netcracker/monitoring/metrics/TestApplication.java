package com.netcracker.monitoring.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TestApplication implements MeterBinder {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    @Override
    public void bindTo(MeterRegistry registry) {
//        registry.config()
//                .meterFilter(MeterFilter.denyNameStartsWith("jvm"))
//                .meterFilter(MeterFilter.denyNameStartsWith("logback"))
//                .meterFilter(MeterFilter.denyNameStartsWith("tomcat"));
    }
}
