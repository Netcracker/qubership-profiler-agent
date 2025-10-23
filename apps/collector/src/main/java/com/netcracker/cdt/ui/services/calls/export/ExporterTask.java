package com.netcracker.cdt.ui.services.calls.export;

import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.tasks.CallsMetaLoader;
import com.netcracker.cdt.ui.services.calls.tasks.LocalReloadTask;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;
import com.netcracker.common.utils.DB;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipOutputStream;

public class ExporterTask {

    private final String serverAddress;
    private final String exportType;
    private final int concurrent;
    private final int maxExportTime;

    private ReloadTaskState state;
    private ZipOutputStream stream;

    public ExporterTask(String serverAddress, String exportType, int concurrent, int maxExportTime) {
        this.serverAddress = serverAddress;
        this.exportType = exportType;
        this.concurrent = concurrent;
        this.maxExportTime = maxExportTime;
    }

    public void stream(ZipOutputStream zout) {
        this.stream = zout;
    }

    @DB("reloadData")
    public void export(CallsMetaLoader metaLoader, CallsListRequest req) throws IOException {

        var recorder = switch (exportType.toLowerCase()) {
            case "csv" -> new CsvCallRecord();
            case "excel" -> new XlsCallRecord(serverAddress);
//            case "excel" -> new XlsExporter();
            default -> throw new IllegalStateException("Invalid export type: " + exportType);
        };

        recorder.appendHeader();
        recorder.print(stream);

        var failed = new AtomicInteger(0);

        var task = new LocalReloadTask(metaLoader, concurrent, req, true) {
            @Override
            protected void proceedSeqResult(CallSeqResult podSeqResult) {
                var task = podSeqResult.subTask();
                if (task == null) return;

                Log.debugf("[%s] Exporting %s with %d calls", task.podId(), task, podSeqResult.parsedCalls());

                podSeqResult.calls().forEach(call -> {
                    recorder.appendRow(call);
                    try {
                        recorder.print(stream);
                    } catch (IOException e) {
                        if (failed.getAndIncrement() == 0) {
                            Log.errorf("[%s#%d] Error during export: %s",
                                    task.podId(), task.sequenceId(), e.getMessage());
                        }
                    }
                });
            }
        };
        this.state = task.prepare()
                .uiLimits(req.pageSize(), req.pageSize())
                .timeout(maxExportTime);
        task.run();

        recorder.flush(stream);
        if (failed.get() > 0) {
            Log.errorf("Got %d errors during export", failed.get());
        }
    }

}
