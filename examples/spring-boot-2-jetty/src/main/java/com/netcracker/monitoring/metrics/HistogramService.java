package com.netcracker.monitoring.metrics;

import io.prometheus.client.Histogram;
import org.springframework.scheduling.annotation.Scheduled;

public class HistogramService {

    public static double value;

    static final Histogram requestLatency = Histogram
            .build()
            .name("requests_latency_seconds")
            .help("Request latency in seconds.")
            .register();

    @Scheduled(fixedDelay = 2000)
    void process() {
        System.out.println("process()");
        Histogram.Timer requestTimer = requestLatency.startTimer();
        try {
            for (int i = 0; i < 10_000_000; i++) {
                double x = i * 0.01;
                value = Math.random() * Math.sin(x);
            }
        } finally {
            requestTimer.observeDuration();
        }
    }
}
