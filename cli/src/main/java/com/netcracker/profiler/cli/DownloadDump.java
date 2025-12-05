package com.netcracker.profiler.cli;

import com.netcracker.profiler.dump.download.DownloadProgress;
import com.netcracker.profiler.dump.download.DownloadProtocol;
import com.netcracker.profiler.dump.download.DumpDownloader;
import com.netcracker.profiler.dump.download.RemoteFile;
import com.netcracker.profiler.io.DurationParser;
import com.netcracker.profiler.io.Pair;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DownloadDump implements Command {
    public static final Logger log = LoggerFactory.getLogger(DownloadDump.class);
    public static final int MAX_PARALLEL_DEGREE = Integer.getInteger(DownloadDump.class.getName() + ".MAX_PARALLEL_DEGREE", 10);
    private final NumberFormat numberFormat = new DecimalFormat("#0.0");
    private long startDate;
    private long endDate;
    private boolean dryRun;
    private boolean skipDetails;
    private String outputFolder;
    private List<String> selectedServers;
    private String dumpRootPath;
    private final Console console = System.console();

    @Override
    public int accept(Namespace args) {
        try (DumpDownloader dumpDownloader = new DumpDownloader()) {
            String host = args.getString("host");
            String user = args.getString("user");
            String password = args.getString("password");
            if(password == null) {
                log.info("Please enter password: ");
                password = String.valueOf(console.readPassword());
            }
            int parallelDegree = args.getInt("parallel");
            String shell = args.getString("shell");

            DownloadProtocol downloadProtocol;
            if(shell == null) {
                String downloadProtocolString = args.getString("download_protocol").toUpperCase();
                try {
                    downloadProtocol = DownloadProtocol.valueOf(downloadProtocolString);
                } catch (IllegalArgumentException e) {
                    log.error("Illegal value for download_protocol={}. It should be SFTP or SCP.", downloadProtocolString);
                    return -1;
                }
            } else {
                log.info("Init shell command is set, so, using SCP download protocol");
                downloadProtocol = DownloadProtocol.SCP;
            }
            log.info("Download protocol is {}", downloadProtocol);
            if(shell != null) {
                log.info("Init shell command is '{}'", shell);
            }
            if(parallelDegree > MAX_PARALLEL_DEGREE) {
                log.info("Parallel degree was set to {}, which is more than max parallel degree = {}", parallelDegree, MAX_PARALLEL_DEGREE);
                log.info("Parallel degree has been downgraded to {}", MAX_PARALLEL_DEGREE);
                parallelDegree = MAX_PARALLEL_DEGREE;
            } else {
                log.info("Parallel degree is {}", parallelDegree);
            }

            log.info("Connecting to {}", host);
            dumpDownloader.connect(host, user, password, parallelDegree, shell, downloadProtocol);
            log.info("Connected");

            String timeZoneStr = args.getString("time_zone");
            TimeZone tz = null;
            if(timeZoneStr == null) {
                log.info("time-zone isn't set, so getting timeZone from server");
                tz = dumpDownloader.getServerTimezone();
                log.info("Server timezone is "+tz.getDisplayName());
            } else {
                tz = TimeZone.getTimeZone(timeZoneStr);
            }

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

            outputFolder = args.getString("output_folder");
            if(outputFolder == null) {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmm");
                outputFolder = "esc_" + fmt.format(new Date(startDate)) + '_' + fmt.format(new Date(endDate));
            }

            skipDetails = args.getBoolean("skip_details");
            if (skipDetails) {
                log.info("Will skip export of trace, sql, xml folders");
            }
            dryRun = args.getBoolean("dry_run");
            if (dryRun) {
                log.info("Running in dry-run mode. No writes will be performed.");
            }

            log.info("Will export results to {}", new File(outputFolder).getAbsolutePath());

            selectedServers = args.getList("server");
            dumpRootPath = args.getString("dump_root");

            return runExport(dumpDownloader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int runExport(DumpDownloader dumpDownloader) throws IOException {
        if(dumpRootPath == null) {
            log.info("Path to dump root isn't set. Trying to find available dump roots in the host.");
            List<String> dumpRoots = dumpDownloader.findDumpRoots();
            if(dumpRoots.isEmpty()) {
                log.error("Couldn't automatically detect dump roots in the server. Please specify it manually via -r argument.");
                return -1;
            } else if(dumpRoots.size() == 1) {
                dumpRootPath = dumpRoots.get(0);
                log.info("Found 1 dump root in the server: {}", dumpRootPath);
            } else {
                log.info("Found the following dump roots in the server. Please choose a number of dump root, which will be used for downloading.");
                StringBuilder logSB = new StringBuilder();
                for(int i = 0; i<dumpRoots.size(); i++) {
                    logSB.append(dumpRoots.get(i)+"("+(i+1)+")\n");
                }
                logSB.delete(logSB.length()-1, logSB.length());
                System.out.println(logSB);
                int n;
                try {
                    n = Integer.parseInt(console.readLine());
                } catch (NumberFormatException e) {
                    log.error("Input value isn't numeric");
                    return -1;
                }
                if(n < 1 || n > dumpRoots.size()) {
                    log.error("Input value should be in range 1 - "+dumpRoots.size());
                    return -1;
                }
                dumpRootPath = dumpRoots.get(n-1);
                log.info("Selected dump root: {}", dumpRootPath);
            }
        }

        log.info("Collecting a list of dump files to download from {}", dumpRootPath);
        List<Pair<RemoteFile, File>> filesToDownload = dumpDownloader.collectFilesToDownload(startDate, endDate, skipDetails, selectedServers, dumpRootPath, outputFolder);
        if (filesToDownload.isEmpty()) {
            log.error("No files found by the specified arguments");
            return -1;
        } else {
            long totalSize = 0;
            for (Pair<RemoteFile, File> file : filesToDownload) {
                totalSize += file.getKey().length();
            }
            if (dryRun) {
                log.info("Dry-run export finished successfully");
            }
            log.info("Found " + filesToDownload.size() + " files with total size " + formatAsMegabytes(totalSize) + " Mb");
            if (dryRun) {
                return 0;
            }
        }
        log.info("Starting downloading files");

        DownloadProgress progress = dumpDownloader.startDownloading();
        long prevBytesDownloaded = 0;
        int i = 0;
        while (!progress.isCompleted()) {
            progress.waitUntilCompleted(5000);
            long bytesDownloaded = progress.getBytesDownloaded();
            log.info("Progress: {}/{} files, {} / {} Mb downloaded", progress.getFilesDownloaded(), progress.getFilesTotal(),
                    formatAsMegabytes(bytesDownloaded), formatAsMegabytes(progress.getBytesTotal()));
            i++;
            if(i == 12) {
                long bytesDownloadedInMinute = bytesDownloaded - prevBytesDownloaded;
                long remainingBytes = progress.getBytesTotal() - bytesDownloaded;
                log.info("Avg speed: "+formatAsMegabytes(bytesDownloadedInMinute/60) +" Mbps, remaining time: "
                        +numberFormat.format((double) remainingBytes / (double) bytesDownloadedInMinute) + " minutes");
                prevBytesDownloaded = progress.getBytesDownloaded();
                i = 0;
            }
        }
        if (!progress.getErrors().isEmpty()) {
            log.error("The following errors happened");
            for (Throwable t : progress.getErrors()) {
                log.error("", t);
            }
            throw new IOException(progress.getErrors().get(0));
        }

        log.info("Successfully exported dump to {}.", outputFolder);

        return 0;
    }

    private String formatAsMegabytes(long bytes) {
        return formatAsMegabytes((double) bytes);
    }

    private String formatAsMegabytes(double bytes) {
        return numberFormat.format(bytes / 1024d / 1024d);
    }
}
