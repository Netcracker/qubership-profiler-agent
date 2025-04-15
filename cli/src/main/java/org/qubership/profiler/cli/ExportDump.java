package org.qubership.profiler.cli;

import static org.qubership.profiler.util.ProfilerConstants.CALL_HEADER_MAGIC;

import org.qubership.profiler.chart.UnaryFunction;
import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.dump.DumpRootResolver;
import org.qubership.profiler.io.DurationParser;
import org.qubership.profiler.sax.readers.ProfilerTraceReaderFile;
import org.qubership.profiler.sax.values.ClobValue;
import org.qubership.profiler.servlet.SpringBootInitializer;
import org.qubership.profiler.util.IOHelper;
import org.qubership.profiler.utils.CommonUtils;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Exports subset of collected data.
 */
public class ExportDump extends ListServers {
    public static final Logger log = LoggerFactory.getLogger(ExportDump.class);
    public static final String DEFAULT_FILE_NAME = "esc_startdate_enddate.zip";

    final static NumberFormat fileIndexFormat = NumberFormat.getIntegerInstance();

    static {
        fileIndexFormat.setGroupingUsed(false);
        fileIndexFormat.setMinimumIntegerDigits(6);
    }

    private long startDate;
    private long endDate;

    private String endPath;

    private boolean dryRun;

    private boolean skipDetails;

    private String fileName;

    private List<String> selectedServers;
    private String currentServer;
    private ZipOutputStream zos;

    int totalFiles;
    long totalBytes;

    private final byte[] tmp = new byte[65536];

    private final static Comparator<Long> LONG_COMPARATOR = new Comparator<Long>() {
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    };

    private static boolean containsOnlyDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    protected final static FileFilter YEAR_DIRECTORY_FILTER = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.isDirectory() && pathname.getName().length() == 4 && containsOnlyDigits(pathname.getName());
        }
    };

    protected final static FileFilter NUMBER_DIRECTORY_FILTER = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.isDirectory() && containsOnlyDigits(pathname.getName());
        }
    };

    private final static UnaryFunction<File, Long> CALLS_START_TIMESTAMP = new UnaryFunction<File, Long>() {
        public Long evaluate(File file) {
            try {
                DataInputStreamEx calls = DataInputStreamEx.openDataInputStream(file);
                if (calls == null) {
                    return System.currentTimeMillis();
                }
                long time = calls.readLong();
                if ((int) (time >>> 32) == CALL_HEADER_MAGIC) {
                    time = calls.readLong();
                }
                if (log.isTraceEnabled()) {
                    log.trace("Timestamp of {} is {} ({})", file.getAbsolutePath(), new Date(time), time);
                }
                return time;
            } catch (EOFException e) {
                return System.currentTimeMillis();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private final static UnaryFunction<File, Long> TRACE_START_TIMESTAMP = new UnaryFunction<File, Long>() {
        public Long evaluate(File file) {
            DataInputStreamEx trace = null;
            try {
                trace = DataInputStreamEx.openDataInputStream(file);
                if (trace == null) {
                    return System.currentTimeMillis();
                }
                trace.readLong(); // serverStart
                trace.readLong(); // threadId
                long realTime = trace.readLong();
                if (log.isTraceEnabled()) {
                    log.trace("Timestamp of {} is {} ({})", file.getAbsolutePath(), new Date(realTime), realTime);
                }
                return realTime;
            } catch (EOFException e) {
                return System.currentTimeMillis();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                IOHelper.close(trace);
            }
        }
    };

    public int accept(Namespace args) {
        setupDumpRoot(args);
        SpringBootInitializer.init();

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
            fileName = "esc_" + fmt.format(new Date(startDate)) + '_' + fmt.format(new Date(endDate)) + ".zip";
        }

        skipDetails = args.getBoolean("skip_details");
        if (skipDetails) {
            log.info("Will skip export of trace, sql, xml folders");
        }
        dryRun = args.getBoolean("dry_run");
        if (dryRun) {
            log.info("Running in dry-run mode. No writes will be performed.");
        }

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
        SimpleDateFormat sdf = new SimpleDateFormat("'" + File.separatorChar + "'yyyy'" + File.separatorChar + "'MM'" + File.separatorChar + "'dd");
        endPath = endDate == Long.MAX_VALUE ? null : sdf.format(endDate) + File.separatorChar + endDate;

        File dumpRoot = getDumpRoot();
        if (dumpRoot == null) {
            log.warn("No dump path found - {}. Please check path to ESC dump (--dump-root)", DumpRootResolver.dumpRoot);
            return -2;
        }

        OutputStream os;
        try {
            if (dryRun)
                os = new OutputStream() {
                    @Override
                    public void write(int b) {
                    }
                };
            else
                os = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            log.error("Unable to open output file " + fileName, e);
            throw e;
        }

        zos = new ZipOutputStream(os);
        zos.setLevel(ZipOutputStream.STORED);
        try {
            log.info("Exporting data from {}", dumpRoot.getAbsolutePath());
            File[] servers = dumpRoot.listFiles(DIRECTORY_FILTER);
            if (servers == null || servers.length == 0) {
                log.warn("No data found in {}. Ensure you set the right --dump-root.", dumpRoot.getAbsolutePath());
                return -2;
            }

            for (File server : servers) {
                currentServer = server.getName();
                if (selectedServers != null && !selectedServers.contains(currentServer)) {
                    log.debug("Skipping server {} since it does not match --server arguments", currentServer);
                    continue;
                }
                log.debug("Exporting data for server {}", currentServer);
                findInFolder(server, "", 0, Long.MAX_VALUE);
            }
        } finally {
            IOHelper.close(zos);
        }
        if (dryRun)
            log.info("Dry-run export finished successfully. The estimated export size is {} ({} MiB)", totalBytes, totalBytes / 1024 / 1024);
        else {
            File outFile = new File(fileName);
            long zipSize = outFile.length();
            log.info("Successfully exported dump to {}. Total export size is {} bytes ({} MiB)"
                    , outFile.getAbsolutePath(), zipSize, zipSize / 1024 / 1024);
        }
        return 0;
    }

    private void findInFolder(File root, String currentPath, int level, long dateUpperBound) throws IOException {
        if (level != 0 && endPath != null && currentPath.compareTo(endPath) > 0) {
            log.trace("Skipping path {}{} since it does not match the required time-frame", currentServer, currentPath);
            return;
        }
        if (level == 4) {
            log.info("Processing {}", root);
            /* We are at root/2010/04/24/123342342 */
            long bytesBefore = totalBytes;
            if (processCalls(currentPath, root, "calls", CALLS_START_TIMESTAMP, dateUpperBound).isEmpty()) {
                log.debug("Ignoring folder {} since no data in calls sub-folder for the required time-frame is found"
                        , root
                );
                return;
            }
            appendFolder(currentPath, root, "dictionary");
            appendFolder(currentPath, root, "params");
            appendFolder(currentPath, root, "suspend");
            if (skipDetails) {
                log.debug("Skipping exporting of trace, sql, and xml folders since --skip-details is used");
            } else {
                List<File> addedTraceFiles = processCalls(currentPath, root, "trace", TRACE_START_TIMESTAMP, dateUpperBound);
                processXmlFiles(currentPath, root, addedTraceFiles);
                appendFolder(currentPath, root, "sql");
            }
            if (totalBytes != bytesBefore) {
                log.info("Added {} bytes ({} MiB total)", totalBytes - bytesBefore, totalBytes / 1024 / 1024);
            }
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("Processing {}, level={}", root.getAbsolutePath(), level);
        }

        if (root.isDirectory()) {
            final File[] files = root.listFiles(level == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
            if (files == null || files.length == 0) {
                return;
            }
            Arrays.sort(files);
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                final String fileName = f.getName();
                final String nextPath = currentPath + File.separatorChar + fileName;
                long upperBound;
                if (level == 3 && i + 1 < files.length) {
                    upperBound = Long.parseLong(files[i + 1].getName());
                } else {
                    File firstDir = findFirstDir(f, level + 1);
                    upperBound = firstDir == null ? Long.MAX_VALUE : Long.parseLong(firstDir.getName());
                }
                findInFolder(f, nextPath, level + 1, upperBound);
            }
        }
    }


    private void processXmlFiles(String folderInZip, File root, List<File> traceFilese)  throws IOException {
        File folder = new File(root, "xml");
        if (!folder.exists()) return;
        folderInZip += File.separatorChar + "xml";

        int[] indexes = readFirstAndLastClobFileIdsFromTraceFiles(traceFilese);
        if (indexes.length!=2) {
            return;
        }
        Arrays.sort(indexes);

        for (int i = indexes[0]; i <= indexes[1]; i++) { //between the first and last index
            File file = new File(folder, fileIndexFormat.format(i) + ".gz");
            appendFile(folderInZip, file);
        }
    }

    private int[] readFirstAndLastClobFileIdsFromTraceFiles(List<File> traceFiles) {
        Set<ClobValue> clobs;
        if (traceFiles == null ||traceFiles.isEmpty()) {
            return new int[0];
        } else if (traceFiles.size() == 1) {
            clobs = ProfilerTraceReaderFile.readClobIdsOnly(traceFiles.get(0), ProfilerTraceReaderFile.ClobReadMode.FIRST_AND_LAST, ProfilerTraceReaderFile.ClobReadTypes.XML_ONLY);
        } else {
            File firstFile = traceFiles.get(0);
            File lastFile = traceFiles.get(traceFiles.size()-1);
            clobs = ProfilerTraceReaderFile.readClobIdsOnly(firstFile, ProfilerTraceReaderFile.ClobReadMode.FIRST_ONLY, ProfilerTraceReaderFile.ClobReadTypes.XML_ONLY);
            clobs.addAll(ProfilerTraceReaderFile.readClobIdsOnly(lastFile, ProfilerTraceReaderFile.ClobReadMode.LAST_ONLY, ProfilerTraceReaderFile.ClobReadTypes.XML_ONLY));
        }

        int[] indexes = getClobFileIndexes(clobs);
        if (indexes.length == 1) {
            indexes = new int[] {indexes[0], indexes[0]};
        }
        return indexes;
    }

    private int[] getClobFileIndexes(Set<ClobValue> clobs) {
        if(clobs == null) return new int[0];
        int[] indexes = new int[clobs.size()];
        int i = 0;
        for(ClobValue clob : clobs) {
            indexes[i] = clob.fileIndex;
            i++;
        }
        return indexes;
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

    private void appendFolder(String folderInZip, File root, String folderName) throws IOException {
        folderInZip += File.separatorChar + folderName;
        File folder = new File(root, folderName);
        File[] files = folder.listFiles();
        if (files == null) return;
        for (File file : files) {
            appendFile(folderInZip, file);
        }
    }

    private List<File> processCalls(String folderInZip, File root, String folderName, UnaryFunction<File, Long> keySelector
            , long dateUpperBound) throws IOException {
        folderInZip += File.separatorChar + folderName;
        File folder = new File(root, folderName);
        if (!folder.exists()) return Collections.emptyList();
        final File[] files = folder.listFiles();
        if (files == null || files.length == 0) return Collections.emptyList();
        Arrays.sort(files);

        if (dateUpperBound < startDate) {
            log.debug("Ignoring folder {} since the estimate of upper bound of stored data is {}, and requested startDate is {}", folder, new Date(dateUpperBound), new Date(startDate));
            return Collections.emptyList();
        }
//   * The method is guaranteed to return the maximal index of the element that is
//   * less or equal to the given key.
        int from = CommonUtils.upperBound(files, startDate, 0, files.length - 1, keySelector, LONG_COMPARATOR);
        if (from == files.length) {
            from--;
        }
        int to = CommonUtils.upperBound(files, endDate, 0, files.length - 1, keySelector, LONG_COMPARATOR);
        if (to == files.length) {
            to--;
        }

        from = Math.max(from, 0);
        to = Math.max(to, 0);
        if (from > to) {
            log.debug("Ignoring folder {} since files look out of range of specified dates", folder.getAbsolutePath());
            return Collections.emptyList();
        }

        List<File> addedFiles = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            File file = files[i];
            appendFile(folderInZip, file);
            addedFiles.add(file);
        }

        return addedFiles;
    }

    private void appendFile(String folder, File file) throws IOException {
        InputStream is;
        String zipFileName = "execution-statistics-collector" + File.separatorChar + "dump"
                + File.separatorChar + currentServer + folder + File.separatorChar + file.getName();

        if (log.isTraceEnabled()) {
            log.trace("Adding file {} as {}", file, zipFileName);
        }

        try {
            is = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            log.warn("Unable to open file " + file.getAbsolutePath(), e);
            return;
        }
        totalFiles++;
        totalBytes += file.length();
        if (dryRun) {
            log.trace("Avoiding file copy since running in dry-run mode {}", file);
            is.close();
            return;
        }
        ZipEntry ze = new ZipEntry(zipFileName);
        ze.setTime(file.lastModified());
        ze.setSize(file.length());
        zos.putNextEntry(ze);
        try {
            int read;
            while ((read = is.read(tmp)) > 0)
                zos.write(tmp, 0, read);
        } finally {
            is.close();
        }
        zos.closeEntry();
    }


    /**
     * Taken from http://en.wikipedia.org/wiki/Binary_search_algorithm
     * #Deferred_detection_of_equality
     */
    public static <T, K> int lowerBound(T[] a, K key, int imin, int imax,
                                        UnaryFunction<T, K> keySelector, Comparator<K> comparator) {
        // continually narrow search until just one element remains
        while (imin < imax) {
            // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=5045582
            int imid = (imin + imax) >>> 1;

            // code must guarantee the interval is reduced at each iteration
            assert imid < imax
                    : "search interval should be reduced min=" + imin
                    + ", mid=" + imid + ", max=" + imax;
            // note: 0 <= imin < imax implies imid will always be less than imax

            // reduce the search
            if (comparator.compare(keySelector.evaluate(a[imid]), key) < 0) {
                // change min index to search upper subarray
                imin = imid + 1;
            } else {
                imax = imid;
            }
        }
        // At exit of while:
        //   if a[] is empty, then imax < imin
        //   otherwise imax == imin

        // deferred test for equality
        if (imax != imin) {
            return -1;
        }

        int cmp = comparator.compare(keySelector.evaluate(a[imin]), key);
        if (cmp == 0) {
            // Detected exact match, just return it
            return imin;
        }
        if (cmp < 0) {
            // We were asked the key that is greater that all the values in array
            return imin + 1;
        }
        // If imin != 0 we return imin since a[imin-1] < key < a[imin]
        // If imin == 0 we return -1 since the resulting window might be empty
        // For instance, range between 100 preceding and 99 preceding
        // Use if-else to ensure code coverage is reported for each return
        if (imin == 0) {
            return -1;
        } else {
            return imin;
        }
    }

}
