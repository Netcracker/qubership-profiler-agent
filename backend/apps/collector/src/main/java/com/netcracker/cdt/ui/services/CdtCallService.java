package com.netcracker.cdt.ui.services;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netcracker.cdt.ui.models.UiServiceConfig;
import com.netcracker.cdt.ui.services.calls.export.ExporterTask;
import com.netcracker.cdt.ui.services.calls.view.ClientWindowInfo;
import com.netcracker.cdt.ui.services.calls.tasks.CallsMetaLoader;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.cdt.ui.services.calls.CallsListResult;
import com.netcracker.common.Time;
import io.quarkiverse.bucket4j.runtime.RateLimited;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.StreamingOutput;

import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.netcracker.cdt.ui.services.CdtDumpsService.FILE_NAME_FORMATTER;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class CdtCallService {

    Cache<String, ClientWindowInfo> cache = Caffeine.newBuilder().softValues().build();

    @Inject
    Time time;
    @Inject
    CallsMetaLoader metaLoader;
    @Inject
    UiServiceConfig config;

    @PostConstruct
    public void init() {
    }

    @RateLimited(bucket = "call")
    public CallsListResult getCallList(CallsListRequest request) {
        var window = cache.get(request.windowId(), this::createNewWindow);

        if (!request.searchHash().equals(window.searchHash())) { // should run different search
            window.reloadData(metaLoader, request.fixUTCRange(), config.getUiConcurrentPods());
        }

        return window.asResponse(request);
    }

    @RateLimited(bucket = "export")
    public CdtDumpsService.OutStream exportCalls(String serverAddress, String exportType, CallsListRequest search) {
        var extension = "excel".equals(exportType) ? "xlsx" : "csv";
        var fileName = "%s.%s".formatted(FILE_NAME_FORMATTER.format(time.now()), extension);
        StreamingOutput stream = outputStream -> {
            var ze = new ZipEntry(fileName);
            var zout = new ZipOutputStream(outputStream);
            zout.putNextEntry(ze);
            zout.flush();

            var export = new ExporterTask(serverAddress, exportType, config.getUiConcurrentPods(), config.getMaxExportTime());
            export.stream(zout);
            export.export(metaLoader, search);

            zout.finish();
            zout.flush();
        };
        return new CdtDumpsService.OutStream(fileName, stream);
    }

    public ClientWindowInfo createNewWindow(String windowId) {
        return new ClientWindowInfo(config, windowId);
    }
}
