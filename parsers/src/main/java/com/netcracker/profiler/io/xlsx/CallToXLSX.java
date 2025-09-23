package com.netcracker.profiler.io.xlsx;

import org.apache.commons.lang.StringEscapeUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CallToXLSX {
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    private final OutputStream out;
    private PrintWriter sheetWriter;

    private int rows = 0;
    private String[] colIndexes;

    private List<Object> rowContent = null;

    public CallToXLSX(OutputStream out) {
        this.out = out;
    }

    public void nextRow() {
        if (rowContent != null) {
            flushRow();
            rowContent.clear();
        } else {
            rowContent = new ArrayList<>();
        }
    }

    public void finish() {
        if (rowContent == null) {
            rowContent = new ArrayList<>();
        }
        flushRow();
        writeSheetFooter(sheetWriter);
        sheetWriter.close();
        sheetWriter = null;
        rows = 0;
    }

    private void flushRow() {
        if (sheetWriter == null) {
            ZipOutputStream zout = new ZipOutputStream(out);
            try {
                addStatic(zout, "docProps/app.xml");
                addStatic(zout, "docProps/core.xml");
                addStatic(zout, "xl/styles.xml");
                addStatic(zout, "xl/workbook.xml");
                addStatic(zout, "xl/_rels/workbook.xml.rels");
                addStatic(zout, "[Content_Types].xml");
                addStatic(zout, "_rels/.rels");
                addStatic(zout, "xl/worksheets/_rels/sheet1.xml.rels");
                zout.putNextEntry(new ZipEntry("xl/worksheets/sheet1.xml"));
                sheetWriter = new PrintWriter(new OutputStreamWriter(zout, StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            colIndexes = colIndexes(rowContent.size());
            writeSheetHeader(sheetWriter, rowContent.size(), colIndexes);
        }

        rows++;
        sheetWriter.append("<row r=\"").append(String.valueOf(rows)).append("\">");
        for (int col = 0; col < rowContent.size(); col++) {
            Object value = rowContent.get(col);
            /*
            if (value instanceof Date) {
                printColumn(col, "d", "<v>", dateFormat.format((Date) value), "</v>");
            } else
            */
            if (value instanceof Number) {
                printColumn(col, "n", "<v>", value.toString(), "</v>");
            } else if (value instanceof String) {
                printColumn(col, "inlineStr", "<is><t>", StringEscapeUtils.escapeXml((String) value), "</t></is>");
            }

        }
        sheetWriter.append("</row>");
    }

    private void printColumn(int col, String type, String prefix, String content, String suffix) {
        sheetWriter.append("<c r=\"").append(colIndexes[col]).append(String.valueOf(rows))
                   .append("\" t=\"").append(type).append("\">")
                   .append(prefix).append(content).append(suffix)
                   .append("</c>");
    }

    public void addText(String text) {
        rowContent.add(text);
    }

    public void addDate(Date date) {
        // Store date as text string
        rowContent.add(dateFormat.format(date));
    }

    public void addNumber(Long number) {
        rowContent.add(number);
    }

    public void addNumber(Integer number) {
        rowContent.add(number);
    }

    public void addNumber(Double number) {
        rowContent.add(number);
    }

    public void addHyperlink(String url) {
        // Hyperlink is not supported for XLSX streaming, so we treat it like regular text
        addText(url);
    }

    public void addEmpty() {
        rowContent.add(null);
    }

    private static String[] colIndexes(int numCols) {
        int radix = 'Z' - 'A' + 1;
        String[] result = new String[numCols];
        for (int i = 0; i < numCols; i++) {
            StringBuilder caption = new StringBuilder();
            int remainder = i;
            //in Excel notation in first register 0-th column is A. 10-th column is AA
            boolean aRepresentsOne = false;
            do {
                caption.append((char) ('A' + remainder % radix - (aRepresentsOne ? 1 : 0)));
                remainder = remainder / radix;
                aRepresentsOne = true;
            } while (remainder > 0);
            result[i] = caption.reverse().toString();
        }
        return result;
    }

    private static void writeSheetHeader(PrintWriter w, int maxCols, String[] colIndex) {
        w.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                 "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"\n" +
                 "           xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">\n" +
                 "    <dimension ref=\"A1:").append(colIndex[maxCols - 1]).append("1").append("\"/>");
        w.append("<sheetViews>" +
                 "<sheetView showFormulas=\"false\" showGridLines=\"true\" showRowColHeaders=\"true\" showZeros=\"true\"" +
                 " rightToLeft=\"false\" tabSelected=\"true\" showOutlineSymbols=\"true\" defaultGridColor=\"true\"" +
                 " view=\"normal\" topLeftCell=\"A1\" colorId=\"64\" zoomScale=\"100\" zoomScaleNormal=\"100\"" +
                 " zoomScalePageLayoutView=\"100\" workbookViewId=\"0\">" +
                 "<pane xSplit=\"1\" ySplit=\"1\" topLeftCell=\"B2\" activePane=\"bottomRight\" state=\"frozen\"/>" +
                 "</sheetView></sheetViews>");
        w.append("<sheetFormatPr defaultRowHeight=\"15.0\"/>");
        printCols(w, maxCols);
        w.append("<sheetData>");
    }

    private static void writeSheetFooter(PrintWriter w) {
        w.append("</sheetData>");
        w.append("<pageMargins bottom=\"0.75\" footer=\"0.3\" header=\"0.3\" " +
                 "left=\"0.7\" right=\"0.7\" top=\"0.75\"/></worksheet>");
    }

    private static void printCols(PrintWriter w, int maxCols) {
        w.append("<cols>");
        for (int i = 0; i < maxCols; i++) {
            w.append("<col min=\"")
             .append(String.valueOf(i + 1))
             .append("\" max=\"").append(String.valueOf(i + 1))
             .append("\" width=\"10\" bestFit=\"1\"/>");
        }
        w.append("</cols>");
    }

    private static void addStatic(ZipOutputStream zout, String resource) throws IOException {
        InputStream resourceStream = CallToXLSX.class.getClassLoader()
                                                     .getResourceAsStream("samplexlsx/" + resource);
        if (resourceStream != null) {
            try (InputStream in = new BufferedInputStream(resourceStream)) {
                zout.putNextEntry(new ZipEntry(resource));
                byte[] buffer = new byte[32768];
                for (int bytesRead = in.read(buffer); bytesRead >= 0; bytesRead = in.read(buffer)) {
                    zout.write(buffer, 0, bytesRead);
                }
            }
        } else {
            throw new IOException("Could not find resource '" + resource + "'");
        }
    }
}
