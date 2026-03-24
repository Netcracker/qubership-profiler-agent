package com.netcracker.monitoring.metrics;

import io.micrometer.core.instrument.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class MetricsController {

    private Helper helper = new Helper(Metrics.globalRegistry, 10000);

    @GetMapping("/err")
    public Map<String, String> errorResponse() {
        return helper.getErrorResponse();
    }

    @GetMapping("/health")
    public Map<String, String> healthResponse() {
        return helper.getStatus();
    }

    @PostMapping("/health")
    public Map<String, String> healthResponse(@RequestParam(name = "status") String status) {
        return helper.updateStatus(status);
    }

    @Scheduled(fixedDelay = 10000)
    private void callRandomEndpoint()  {
        helper.emulateRandomEndpoint();
    }
}
