package com.netcracker.profiler.cli;


import com.netcracker.profiler.guice.DumpRootLocation;
import com.netcracker.profiler.io.DurationParser;
import com.netcracker.profiler.io.ExcelExporter;
import com.netcracker.profiler.io.TemporalRequestParams;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Exports subset of collected data.
 */
@Singleton
public class ExportExcel implements Command {
    public static final Logger log = LoggerFactory.getLogger(ExportExcel.class);
    public static final String DEFAULT_FILE_NAME = "esc_calls_startdate_enddate.xlsx";

    private long startDate;
    private long endDate;
    private String fileName;
    private List<String> selectedServers;

    private boolean aggregate;
    private int minDuration;
    private int minDigitsInId;
    private boolean disableDefaultUrlReplacePatterns;
    List<String> urlReplacePatterns;
    private final File dumpRoot;
    private final ExcelExporter excelExporter;

    @Inject
    public ExportExcel(@DumpRootLocation File dumpRoot, ExcelExporter excelExporter) {
        this.dumpRoot = dumpRoot;
        this.excelExporter = excelExporter;
    }

    public int accept(Namespace args) {
        TimeZone tz = TimeZone.getTimeZone(args.getString("time_zone"));

        String endDateStr = args.getString("end_date");
        String startDateStr = args.getString("start_date");

        endDate = DurationParser.parseTimeInstant(endDateStr, Long.MAX_VALUE, Long.MAX_VALUE, tz);
        startDate = DurationParser.parseTimeInstant(startDateStr, Long.MAX_VALUE, endDate, tz);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm z");
        sdf.setTimeZone(tz);
        log.info("Exporting the data from {} to {}", sdf.format(new Date(startDate)), sdf.format(new Date(endDate)));

        long now = System.currentTimeMillis();
        if (startDate > now) {
            log.error("--start-date and --end-date are in the future. Please clarify the arguments and retry.");
            return -1;
        }

        fileName = args.getString("output_file");
        if (DEFAULT_FILE_NAME.equals(fileName)) {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmm");
            fileName = "esc_calls_" + fmt.format(new Date(startDate)) + '_' + fmt.format(new Date(endDate)) + ".xlsx";
        }

        aggregate = args.getBoolean("aggregate");
        minDuration = args.getInt("min_duration");
        minDigitsInId = args.getInt("min_digits_in_id");
        disableDefaultUrlReplacePatterns = args.getBoolean("disable_default_url_replace_patterns");
        urlReplacePatterns = args.getList("url_replace_pattern");

        File file = new File(fileName);
        log.info("Will export results to {}", file.getAbsolutePath());

        selectedServers = args.getList("server");

        try {
            return runExport();
        } catch (IOException e) {
            log.error("Error while exporting data", e);
            return -1;
        }
    }

    private int runExport() throws IOException {
        System.setProperty("com.netcracker.profiler.agent.Profiler.MAX_CALLS_FOR_AGGREGATE_TO_EXCEL", "-1");
        System.setProperty("com.netcracker.profiler.agent.Profiler.MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL", "-1");
        if (dumpRoot == null) {
            log.warn("No dump path found. Please check path to ESC dump (--dump-root)");
            return -2;
        }

        try(OutputStream os = new FileOutputStream(fileName)) {
            log.info("Exporting data from {}", dumpRoot.getAbsolutePath());

            long now = System.currentTimeMillis();
            TemporalRequestParams temporal = new TemporalRequestParams(now, now, now, startDate, endDate, 1, minDuration, Long.MAX_VALUE);
            Map<String, String[]> params = new HashMap<>();
            params.put("type", new String[] {aggregate ? "aggregate" : "all"});
            params.put("minDigitsInId", new String[] {String.valueOf(minDigitsInId)});
            params.put("urlReplacePatterns", urlReplacePatterns == null ? null : urlReplacePatterns.toArray(new String[] {}));
            params.put("disableDefaultUrlReplacePatterns", new String[] {String.valueOf(disableDefaultUrlReplacePatterns)});
            params.put("nodes", selectedServers == null ? null : selectedServers.toArray(new String[] {}));

            excelExporter.export(temporal, params, os);
        } catch (FileNotFoundException e) {
            log.error("Unable to open output file " + fileName, e);
            throw e;
        }
        return 0;
    }

}
