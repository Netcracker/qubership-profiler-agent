package com.netcracker.cdt.ui.services.calls.export;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class XlsCallRecord implements AutoCloseable, ExportRecord {
    private static int MEMORY_ROWS = 100; // keep 100 rows in memory, exceeding rows will be flushed to disk
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

    private final String serverAddress;
    private final SXSSFWorkbook workbook;
    private final Sheet sheet;
    private final CellStyle linkStyle, headerStyle;
    private int rownum;

    public XlsCallRecord(String serverAddress) {
        this.serverAddress = serverAddress;
        this.workbook = new SXSSFWorkbook(MEMORY_ROWS);
        this.sheet = workbook.createSheet();

        linkStyle = workbook.createCellStyle();
        var linkFont = workbook.createFont();
        linkFont.setUnderline(Font.U_SINGLE);
        linkFont.setColor(HSSFColor.HSSFColorPredefined.BLUE_GREY.getIndex());
        linkStyle.setFont(linkFont);

        headerStyle = workbook.createCellStyle();
        var headerFont = workbook.createFont();
        headerFont.setBold(true);
        headerStyle.setFont(headerFont);
    }

    public void print(OutputStream out) throws IOException { // after each row/header
    }

    public void flush(OutputStream out) throws IOException { // before closing
        workbook.write(out);
    }

    public void appendHeader() {
        createHeaders("Link", "Start timestamp",
                "Duration", "CPU Time(ms)", "Suspended(ms)", "Queue(ms)",
                "Calls", "Transactions", "Disk Read (B)", "Disk Written (B)", "RAM (B)",
                "Logs generated", "Logs written (B)", "Net read (B)", "Net written (B)",
                "Namespace", "Service Name", "POD", "Method");
    }

    private void createHeaders(String ...values) {
        var headerRow = sheet.createRow(rownum++);
        for (int i = 0; i < values.length; i++) {
            var cell = headerRow.createCell(i);
            cell.setCellValue(values[i]);
            cell.setCellStyle(headerStyle);
        }
    }

    public void appendRow(CallRecord call) {
        int cellIndex = 0;
        var row = sheet.createRow(rownum++);

        var linkHref = "%s/esc/tree.html#params-trim-size=15000&f[_0]=%s&i=0_%s".
                formatted(serverAddress, call.pod().oldPodName(), call.traceRecordId());
        var linkToCall = sheet.getWorkbook().getCreationHelper().createHyperlink(HyperlinkType.URL);
        linkToCall.setAddress(linkHref);

        var detailsCell = row.createCell(cellIndex++);
        detailsCell.setHyperlink(linkToCall);
        detailsCell.setCellValue("details");
        detailsCell.setCellStyle(linkStyle);

        var timestamp = formatter.format(Instant.ofEpochMilli(call.actualTimestamp()));
        row.createCell(cellIndex++).setCellValue(timestamp);

        row.createCell(cellIndex++).setCellValue(call.actualDuration());
        row.createCell(cellIndex++).setCellValue(call.cpuTime());
        row.createCell(cellIndex++).setCellValue(call.suspendDuration());
        row.createCell(cellIndex++).setCellValue(call.queueWaitDuration());
        row.createCell(cellIndex++).setCellValue(call.calls());
        row.createCell(cellIndex++).setCellValue(call.transactions());
        row.createCell(cellIndex++).setCellValue(call.fileRead());
        row.createCell(cellIndex++).setCellValue(call.fileWritten());
        row.createCell(cellIndex++).setCellValue(call.memoryUsed());
        row.createCell(cellIndex++).setCellValue(call.logsGenerated());
        row.createCell(cellIndex++).setCellValue(call.logsWritten());
        row.createCell(cellIndex++).setCellValue(call.netRead());
        row.createCell(cellIndex++).setCellValue(call.netWritten());
        row.createCell(cellIndex++).setCellValue(call.pod().namespace());
        row.createCell(cellIndex++).setCellValue(call.pod().service());
        row.createCell(cellIndex++).setCellValue(call.pod().podName());
        row.createCell(cellIndex).setCellValue(call.method());

    }

    @Override
    public void close() throws Exception {
        try {
            workbook.dispose(); // dispose of temporary files backing this workbook on disk
        } finally {
            workbook.close();
        }
    }
}
