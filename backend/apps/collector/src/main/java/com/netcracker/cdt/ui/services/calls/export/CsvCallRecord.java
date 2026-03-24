package com.netcracker.cdt.ui.services.calls.export;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CsvCallRecord implements ExportRecord {
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

    private final StringBuilder builder;

    public CsvCallRecord() {
        this.builder = new StringBuilder(); // TODO use pool of StringBuilder and sb.setLength(0);
    }

    public CsvCallRecord(StringBuilder b) {
        this.builder = b;
    }

    public void print(OutputStream out) throws IOException { // after each row/header
        var s = builder.toString();
        out.write(s.getBytes(StandardCharsets.UTF_8));
        builder.setLength(0);
    }

    public void flush(OutputStream out) throws IOException { // before closing
    }

    public void appendHeader() {
        builder.append("Start timestamp ; ");
        builder.append("Duration ; ");
        builder.append("CPU Time(ms) ; ");
        builder.append("Suspended(ms) ; ");
        builder.append("Queue(ms) ; ");
        builder.append("Calls ; ");
        builder.append("Transactions ; ");
        builder.append("Disk Read (B) ; ");
        builder.append("Disk Written (B) ; ");
        builder.append("RAM (B) ; ");
        builder.append("Logs generated ; ");
        builder.append("Logs written (B) ; ");
        builder.append("Net read (B) ; ");
        builder.append("Net written (B) ; ");
        builder.append("Namespace ; ");
        builder.append("Service Name ; ");
        builder.append("POD ; ");
        builder.append("method");
        builder.append("\n");
    }

    public void appendRow(CallRecord call) {
        append(formatter.format(Instant.ofEpochMilli(call.actualTimestamp())));
        append(call.actualDuration());
        append(call.cpuTime());
        append(call.suspendDuration());
        append(call.queueWaitDuration());
        append(call.calls());
        append(call.transactions());
        append(call.fileRead());
        append(call.fileWritten());
        append(call.memoryUsed());
        append(call.logsGenerated());
        append(call.logsWritten());
        append(call.netRead());
        append(call.netWritten());
        append(call.pod().namespace());
        append(call.pod().service());
        append(call.pod().podName());
        append(call.method());
        builder.append("\n");
    }

    private void append(long v) {
        builder.append(v);
        builder.append(" ; ");
    }

    private void append(String v) {
        builder.append(v);
        builder.append(" ; ");
    }
}
