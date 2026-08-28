package com.netcracker.profiler.cli;

import static com.netcracker.profiler.cli.ExportDump.NUMBER_DIRECTORY_FILTER;
import static com.netcracker.profiler.cli.ExportDump.YEAR_DIRECTORY_FILTER;

import com.netcracker.profiler.guice.DumpRootLocation;
import com.netcracker.profiler.io.DurationParser;
import com.netcracker.profiler.io.xlsx.CallToXLSX;
import com.netcracker.profiler.sax.readers.InFlightCallsFromTraceScanner;
import com.netcracker.profiler.sax.readers.InFlightCallsFromTraceScannerFactory;
import com.netcracker.profiler.utils.CommonUtils;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

import jakarta.inject.Inject;

public class ExportInFlightCalls implements Command {
    public static final Logger log = LoggerFactory.getLogger(ExportInFlightCalls.class);
    public static final String DEFAULT_FILE_NAME = "esc_inflight_calls_startdate+.xlsx";

    private long startDate;
    private String fileName;
    private String nodeName;
    private final File dumpRoot;
    private final InFlightCallsFromTraceScannerFactory scannerFactory;

    @Inject
    public ExportInFlightCalls(@DumpRootLocation File dumpRoot, InFlightCallsFromTraceScannerFactory scannerFactory) {
        this.dumpRoot = dumpRoot;
        this.scannerFactory = scannerFactory;
    }

    public int accept(Namespace args) {
        TimeZone tz = TimeZone.getTimeZone(args.getString("time_zone"));

        String startDateStr = args.getString("start_date");

        startDate = startDateStr == null ? Long.MIN_VALUE : DurationParser.parseTimeInstant(startDateStr, Long.MAX_VALUE, Long.MAX_VALUE, tz);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm z");
        sdf.setTimeZone(tz);
        log.info("Will scan inFlight calls from {}", sdf.format(new Date(startDate)));

        long now = System.currentTimeMillis();
        if (startDate > now) {
            log.error("--start-date is in the future. Please clarify the arguments and retry.");
            return -1;
        }

        fileName = args.getString("output_file");
        if (DEFAULT_FILE_NAME.equals(fileName)) {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmm");
            fileName = "esc_inflight_calls_" + fmt.format(new Date(startDate)) + "+.xlsx";
        }

        File file = new File(fileName);
        log.info("Will export results to {}", file.getAbsolutePath());

        nodeName = args.getString("server");

        try {
            return runExport();
        } catch (IOException e) {
            log.error("Error while exporting data", e);
            return -1;
        }
    }

    private int runExport() throws IOException {
        if (dumpRoot == null) {
            log.warn("No dump path found - {}. Please check path to ESC dump (--dump-root)", dumpRoot);
            return -2;
        }
        File dumpRootNode = new File(dumpRoot, nodeName);

        try(OutputStream os = new FileOutputStream(fileName)) {
            log.info("Scanning inFlight calls from {}", dumpRootNode.getAbsolutePath());
            File startTraceFile = findInFolder(dumpRootNode, 0, Long.MAX_VALUE);
            if(startTraceFile == null || !startTraceFile.exists()) {
                log.error("Cannot find start trace file");
                return -2;
            }
            String rootReference = getRelativePath(startTraceFile.getParentFile().getParentFile(), dumpRoot);
            int startFileIndex = Integer.parseInt(startTraceFile.getName().replace(".gz", ""));
            log.info("Will scan trace files starting from {}", startTraceFile);
            InFlightCallsFromTraceScanner scanner = scannerFactory.create(rootReference, startFileIndex);
            List<InFlightCallsFromTraceScanner.CallInfo> calls = scanner.find();
            printCallsToXlsx(calls, rootReference, os);
        } catch (FileNotFoundException e) {
            log.error("Unable to open output file " + fileName, e);
            throw e;
        }
        return 0;
    }

    private File findInFolder(File root, int level, long dateUpperBound) throws IOException {
        if (level == 4) {
            log.info("Processing {}", root);
            /* We are at root/2010/04/24/123342342 */
            if (dateUpperBound < startDate) {
                log.debug("Ignoring folder {} since the estimate of upper bound of stored data is {}, and requested startDate is {}", root, new Date(dateUpperBound), new Date(startDate));
                return null;
            }

            return findTraceFile(new File(root, "trace"));
        }

        if (root.isDirectory()) {
            final File[] files = root.listFiles(level == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
            if (files == null || files.length == 0) {
                return null;
            }
            Arrays.sort(files);
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                final String fileName = f.getName();
                long upperBound;
                if (level == 3 && i + 1 < files.length) {
                    upperBound = Long.parseLong(files[i + 1].getName());
                } else {
                    File firstDir = findFirstDir(f, level + 1);
                    upperBound = firstDir == null ? Long.MAX_VALUE : Long.parseLong(firstDir.getName());
                }
                File file = findInFolder(f, level + 1, upperBound);
                if(file != null) {
                    return file;
                }
            }
        }

        return null;
    }

    private File findFirstDir(File root, int level) {
        if (level == 0) {
            return null;
        }
        File[] files = root.getParentFile().listFiles(level - 1 == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
        if (files == null || files.length == 0) {
            return null;
        }
        String rootName = root.getName();
        File next = null;
        String nextName = null;
        for (File file : files) {
            String name = file.getName();
            if (name.compareTo(rootName) <= 0) {
                continue;
            }
            if (nextName == null || nextName.compareTo(name) > 0) {
                next = file;
                nextName = name;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Next folder for {} is {}", root, next);
        }

        if (next == null) {
            return findFirstDir(root.getParentFile(), level - 1);
        }

        return findFirstLogDir(next, level);
    }

    private File findFirstLogDir(File root, int level) {
        log.debug("Searching for the first log directory in {}, level {}", root, level);

        if (root.isDirectory()) {
            final File[] files = root.listFiles(level == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
            if (files == null || files.length == 0) {
                log.debug("Folder {} has no files", root);
                return null;
            }
            if (level == 3) {
                File min = Collections.min(Arrays.asList(files));
                log.debug("Minimal file in root {} is {}", root, min);
                return min;
            }
            Arrays.sort(files);
            for (File f : files) {
                File res = findFirstLogDir(f, level + 1);
                if (res != null) {
                    return res;
                }
            }
        }
        return null;
    }

    private File findTraceFile(File folder) throws IOException {
        if (!folder.exists()) return null;
        final File[] files = folder.listFiles();
        if (files == null || files.length == 0) return null;
        Arrays.sort(files);
//   * The method is guaranteed to return the maximal index of the element that is
//   * less or equal to the given key.
        int idx = CommonUtils.upperBound(files, startDate, 0, files.length - 1, CommonUtils::getTraceStartTimestamp, Long::compare);
        if (idx == files.length) {
            idx--;
        }

        idx = Math.max(idx, 0);
        return files[idx];
    }

    private void printCallsToXlsx(List<InFlightCallsFromTraceScanner.CallInfo> calls, String rootReference, OutputStream os) {
        CallToXLSX formatter = new CallToXLSX(os);
        formatter.nextRow();
        formatter.addText("Link");
        formatter.addText("Start timestamp");
        formatter.addText("Duration");
        formatter.addText("Method");

        String serverName = rootReference;
        // Extract server name from rootReference (e.g. clust1/2022/08/03/1659478612505)
        if (rootReference.length() > 25 &&
                rootReference.substring(rootReference.length() - 25).matches("[/\\\\]\\d{4}[/\\\\]\\d{2}[/\\\\]\\d{2}[/\\\\]\\d{13}")) {
            serverName = rootReference.substring(0, rootReference.length() - 25);
        }

        for(InFlightCallsFromTraceScanner.CallInfo call : calls) {
            formatter.nextRow();
            formatter.addHyperlink("/tree.html#params-trim-size=15000&f%5B_0%5D=" + encodeURL(rootReference.replace('\\', '/')) +
                    "&i=" + "0_" + call.traceFileIndex + "_" + call.bufferOffset + "_" + call.recordIndex
            );
            formatter.addDate(new Date(call.startTime));
            formatter.addNumber(call.duration);
            formatter.addText(call.methodName);
        }
        formatter.finish();
    }

    private String encodeURL(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (Exception e) {
            return value;
        }
    }

    private String getRelativePath(File dumpDirFile, File dumpRoot) {
        return dumpDirFile.getAbsolutePath().substring(dumpRoot.getAbsolutePath().length() + 1); // server_name/2010/06/10/123123123
    }

}
