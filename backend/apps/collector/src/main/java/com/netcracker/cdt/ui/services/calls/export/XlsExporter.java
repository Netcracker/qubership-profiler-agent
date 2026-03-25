package com.netcracker.cdt.ui.services.calls.export;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.*;

public class XlsExporter {
    public static int MEMORY_ROWS = 100; // keep 100 rows in memory, exceeding rows will be flushed to disk

    public void export(String name) {
        try (FileOutputStream out = new FileOutputStream(name)) {
            export(out);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void export(OutputStream out) throws IOException {
        try (SXSSFWorkbook wb = new SXSSFWorkbook(MEMORY_ROWS)) {
            try {
                Sheet sh = wb.createSheet();
                for (int rownum = 0; rownum < 1000; rownum++) {
                    Row row = sh.createRow(rownum);
                    for (int cellnum = 0; cellnum < 10; cellnum++) {
                        Cell cell = row.createCell(cellnum);
                        String address = new CellReference(cell).formatAsString();
                        cell.setCellValue(address);
                    }
                }
                wb.write(out);
            } finally {
                wb.dispose(); // dispose of temporary files backing this workbook on disk
            }
        }
    }
}
