package com.netcracker.monitoring.metrics;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/**
 * Endpoints that exercise the calls the profiler agent hooks: an incoming request, a JDBC query,
 * and an outgoing HTTP call.
 */
@RestController
public class DemoController {

    private final JdbcTemplate jdbcTemplate;
    private final int serverPort;
    // The agent hooks RestTemplate over HttpURLConnection only, so the factory is set explicitly.
    private final RestTemplate restTemplate = new RestTemplate(new SimpleClientHttpRequestFactory());

    public DemoController(JdbcTemplate jdbcTemplate, @Value("${server.port:8080}") int serverPort) {
        this.jdbcTemplate = jdbcTemplate;
        this.serverPort = serverPort;
    }

    @GetMapping("/demo/ping")
    public String ping() {
        return "pong";
    }

    @GetMapping("/demo/db")
    public String db() {
        return jdbcTemplate.queryForObject("select 'profiler-it-sb4'", String.class);
    }

    @GetMapping("/demo/outgoing")
    public String outgoing() {
        return restTemplate.getForObject("http://localhost:" + serverPort + "/demo/ping", String.class);
    }
}
