package com.netcracker.cdt.ui.models;

import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class UiServiceConfig {

    @ConfigProperty(name = "ui.concurrent.pods", defaultValue="20") // UI_CONCURRENT_PODS
    int uiConcurrentPods;

    @ConfigProperty(name = "ui.first.calls", defaultValue="100") // UI_FIRST_CALLS
    int uiFirstPage;

    @ConfigProperty(name = "ui.max.calls", defaultValue="4000") // UI_MAX_CALLS
    int uiMaxLimit;

    @ConfigProperty(name = "ui.max.request.time", defaultValue="5000") // UI_MAX_REQUEST_TIME
    int maxRequestTime;

    @ConfigProperty(name = "ui.max.export.rows", defaultValue="1000000") // UI_MAX_EXPORT_ROWS
    int maxExportRows;

    @ConfigProperty(name = "ui.max.export.time", defaultValue="25000") // UI_MAX_EXPORT_TIME
    int maxExportTime;

    public int getUiConcurrentPods() {
        return uiConcurrentPods;
    }

    public int getUiFirstPage() {
        return uiFirstPage;
    }

    public int getUiMaxLimit() {
        return uiMaxLimit;
    }

    public int getMaxRequestTime() {
        return maxRequestTime;
    }

    public int getMaxExportTime() {
        return maxExportTime;
    }

    public int getMaxExportRows() {
        return maxExportRows;
    }
}
