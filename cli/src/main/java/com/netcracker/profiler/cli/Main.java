package com.netcracker.profiler.cli;

import static com.netcracker.profiler.cli.ExportDump.NUMBER_DIRECTORY_FILTER;
import static com.netcracker.profiler.cli.ExportDump.YEAR_DIRECTORY_FILTER;

import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.guice.ParsersModule;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.google.inject.Guice;
import com.google.inject.Injector;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.*;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Entry point to ESC command-line interface.
 */
public class Main {
    public static final String COMMAND_ID = "_command_";
    private static final String DATE_FORMATS_EPILOG = "Valid formats for date are:\n" +
            "    AmonthBweekCdayDhourEminute (e.g. 5h30min means 5h30min ago)\n" +
            "    YYYY-MM-DD HH24:MI\n" +
            "    MM-DD HH24:MI\n" +
            "    HH24:MI\n" +
            "    unix timestamp (number of (milli-)seconds since 1970)\n" +
            "";

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("esc-cmd.sh").build();
        parser.addArgument("-v", "--verbose").action(Arguments.count())
                .help("verbose output, use -v -v for more verbose output");

        parser.defaultHelp(true);
        Subparsers subparsers = parser.addSubparsers()
                .help("use COMMAND --help to get the help on particular command")
                .title("valid subcommands");

        Subparser listServers = subparsers
                .addParser("list-servers")
                .defaultHelp(true)
                .help("list valid server names for export-dump command")
                .setDefault(COMMAND_ID, ListServers.class);
        addDumpRootArg(listServers);

        Subparser exportDump = subparsers
                .addParser("export-dump")
                .help("Export the collected data for the specified time-frame to a separate archive");
        exportDump.addArgument("-d", "--dry-run").action(Arguments.storeTrue())
                .help("skips export, just scans the folders and prints the estimated size of the archive");
        exportDump.addArgument("-q", "--skip-details").action(Arguments.storeTrue())
                .help("exports only high-level information, skips export of profiling trees");
        addExportArgs(exportDump, ExportDump.class, ExportDump.DEFAULT_FILE_NAME, false, TimeZone.getDefault().getID());
        addDumpRootArg(exportDump);

        Subparser exportExcel = subparsers
                .addParser("export-excel")
                .help("Export profiler calls for the specified time-frame to excel");
        exportExcel.addArgument("-a", "--aggregate").help("generates aggregate report instead of exporting of all calls")
                .action(Arguments.storeTrue());
        exportExcel.addArgument("-d", "--min-duration").metavar("DURATION").help("specifies the minimum duration for exporting of calls in ms")
                .type(Integer.class).setDefault(500);
        exportExcel.addArgument("-md", "--min-digits-in-id").metavar("NUMBER").help("specifies the minimum digits in a part of URL (not necessarily sequintial) to consider it as Id and replace to $id$. Set it to -1 to disable it.")
                .type(Integer.class).setDefault(4);
        exportExcel.addArgument("-du", "--disable-default-url-replace-patterns").help("disables default url replacement patterns")
                .action(Arguments.storeTrue());
        exportExcel.addArgument("-ur", "--url-replace-pattern").action(Arguments.append())
                .metavar("PATTERN").help("specifies the custom url replace pattern, used for replacement of IDs. Value should be placed in '' for proper handling. Mulitple -ur arguments are possible.\n" +
                        "* : matches everything except /\n" +
                        "** : matches everything\n" +
                        "$id$ : the same as *, but matched symbols will be replaced to $id$ in result. It should be used for replacing of ids.\n" +
                        "Examples: /api/csrd/threesixty/$id$/*\n" +
                        "**/wfm/appointment/$id$/*");
        addExportArgs(exportExcel, ExportExcel.class, ExportExcel.DEFAULT_FILE_NAME, false, TimeZone.getDefault().getID());
        addDumpRootArg(exportExcel);

        Subparser downloadDump = subparsers
                .addParser("download-dump")
                .help("Download the collected data for the specified time-frame to local folder");
        downloadDump.addArgument("-d", "--dry-run").action(Arguments.storeTrue())
                .help("skips export, just scans the folders and prints the estimated size of the archive");
        downloadDump.addArgument("-q", "--skip-details").action(Arguments.storeTrue())
                .help("exports only high-level information, skips export of profiling trees");
        downloadDump.addArgument("-rh", "--host").metavar("HOST").required(true).help("Remote host for remote connection");
        downloadDump.addArgument("-u", "--user").metavar("USER").required(true).help("Username for remote connection");
        downloadDump.addArgument("-p", "--password").metavar("PASSWORD").help("Password for remote connection.\n"+
                "If not set, then user will be prompted to enter password during execution.");
        downloadDump.addArgument("-pd", "--parallel").metavar("DEGREE").type(Integer.class).setDefault(1).help("Parallel degree. Max is 10.");
        downloadDump.addArgument("-sh", "--shell").metavar("SHELL").help("Init shell command to execute after SSH session is created.\n" +
                "Can be used to login as a local user. For ex: sudo su - netcrk");
        downloadDump.addArgument("-dp", "--download-protocol").metavar("PROTOCOL").setDefault("SFTP")
                .help("Protocol, which will be used for downloading files. Possible values are: SFTP, SCP. Default is SFTP or SCP if local-user is set");
        addExportArgs(downloadDump, DownloadDump.class, null, true, null);
        downloadDump.addArgument("-r", "--dump-root").metavar("PATH")
                .help("Root folder to gather collected data from.\n" +
                        "If not set, then it will try to automatically detect it.");

        Subparser exportInFlightCalls = subparsers
                .addParser("export-inflight-calls")
                .aliases("export-inflights", "export-ic")
                .help("Scans trace files and exports inFlight calls to excel.")
                .defaultHelp(true)
                .epilog(DATE_FORMATS_EPILOG)
                .setDefault(COMMAND_ID, ExportInFlightCalls.class);
        exportInFlightCalls.addArgument("output_file").metavar("OUTPUT_FILE").help("specifies the name of the output file")
                .nargs("?")
                .setDefault(ExportInFlightCalls.DEFAULT_FILE_NAME);
        exportInFlightCalls.addArgument("-s", "--start-date").metavar("DATE")
                .required(true)
                .help("specifies the start timestamp to scan trace files.\n" +
                        "Scan will be done from the the specified timestamp till the end of a dump or till server reboot after this timestamp.\n" +
                        "If an inFlight call was started before the specified timestamp, then you will get a link to a sub-tree of call, started from specified timestamp.");
        exportInFlightCalls.addArgument("-z", "--time-zone").metavar("TIME_ZONE").help("specifies time zone to disambiguate timestamp arguments. either an abbreviation"
                        + " such as \"PST\", a full name such as \"America/Los_Angeles\", or a custom"
                        + " ID such as \"GMT-8:00\"GMT zone if the given ID is not understood")
                .setDefault(TimeZone.getDefault().getID());
        exportInFlightCalls.addArgument("-n", "--server").metavar("SEVER_NAME")
                .required(true)
                .help("exports inFlight calls from the specified node.");
        addDumpRootArg(exportInFlightCalls);

        Subparser repairProfiler = subparsers
                .addParser("repair-profiler")
                .help("Repairs static content of anonymized profiler html page. Tree data won't be repaired if it's broken.")
                .defaultHelp(true)
                .setDefault(COMMAND_ID, RepairProfiler.class);
        repairProfiler.addArgument("input_file").metavar("INPUT_FILE").help("specifies name of the input file")
                .nargs("?")
                .required(true);
        repairProfiler.addArgument("output_file").metavar("OUTPUT_FILE").help("specifies name of the output file")
                .nargs("?")
                .setDefault(RepairProfiler.DEFAULT_FILE_NAME);

        if (args.length == 0) {
            parser.printHelp();
            return;
        }

        Namespace ns = parser.parseArgsOrFail(args);

        configureLogger(ns);

        Injector injector = Guice.createInjector(new ParsersModule(getDumpServerLocation(ns)));

        Class<? extends Command> cmdClass = ns.get(COMMAND_ID);
        Command cmd = injector.getInstance(cmdClass);

        int code = cmd.accept(ns);
        if (code != 0) {
            System.exit(code);
        }
    }

    private static File getDumpServerLocation(Namespace args) {
        String dumpRoot = args.getString("dump_root");
        if (dumpRoot == null) {
            return null;
        }
        File root = new File(dumpRoot);
        if ("dump".equals(root.getName())) {
            dumpRoot += File.separatorChar + "default";
        } else if (new File(root, "dump").exists()) {
            dumpRoot += File.separatorChar + "dump" + File.separatorChar + "default";
        }
        return new File(dumpRoot);
    }

    private static final Set<String> DUMP_FOLDERS = new HashSet<>(
            Arrays.asList("calls", "dictionary", "params", "sql", "suspend", "trace", "xml")
    );

    protected void setupDumpRoot(Namespace args) throws IOException {
        String dumpRoot = args.getString("dump_root");
        if (dumpRoot != null) {
            File root = new File(dumpRoot);
            if(root.exists()) {
                if ("dump".equals(root.getName())) {
                    dumpRoot += File.separatorChar + "default";
                } else if (new File(root, "dump").exists()) {
                    dumpRoot += File.separatorChar + "dump" + File.separatorChar + "default";
                } else if(!isDumpNodeFolder(root)) {
                    dumpRoot += File.separatorChar + "default";
                }
            }
            DumpRootResolver.dumpRoot = new File(dumpRoot).getCanonicalPath();
        }
    }

    private boolean isDumpNodeFolder(File file) {
        for (File year : emptyIfNull(file.listFiles(YEAR_DIRECTORY_FILTER))) {
            for (File month : emptyIfNull(year.listFiles(NUMBER_DIRECTORY_FILTER))) {
                for (File day : emptyIfNull(month.listFiles(NUMBER_DIRECTORY_FILTER))) {
                    for (File timestamp : emptyIfNull(day.listFiles(NUMBER_DIRECTORY_FILTER))) {
                        if (hasDumpFolder(timestamp)) return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean hasDumpFolder(File timestamp) {
        for (File dumpDir : emptyIfNull(timestamp.listFiles(File::isDirectory))) {
            if (DUMP_FOLDERS.contains(dumpDir.getName())) {
                return true;
            }
        }
        return false;
    }

    private File[] emptyIfNull(File[] files) {
        if(files == null) {
            return new File[0];
        } else {
            return files;
        }
    }

    private static Subparser addExportArgs(Subparser subparser, Class<? extends Command> command, String defaultOutputFileName, boolean useOutputFolder, String defaultTimeZone) {
        subparser = subparser
                .defaultHelp(true)
                .epilog(DATE_FORMATS_EPILOG);
        subparser.setDefault(COMMAND_ID, command);
        if(useOutputFolder) {
            subparser.addArgument("output_folder").metavar("OUTPUT_FOLDER").help("specifies target path, to where download dump")
                    .nargs("?")
                    .setDefault(defaultOutputFileName);
        } else {
            subparser.addArgument("output_file").metavar("OUTPUT_FILE").help("specifies the name of the output file")
                    .nargs("?")
                    .setDefault(defaultOutputFileName);
        }
        subparser.addArgument("-s", "--start-date").metavar("DATE").help("specifies the start timestamp of the export time-frame")
                .setDefault("1hour");
        subparser.addArgument("-e", "--end-date").metavar("DATE").help("specifies the end timestamp of the export time-frame")
                .setDefault("now");
        subparser.addArgument("-z", "--time-zone").metavar("TIME_ZONE").help("specifies time zone to disambiguate timestamp arguments. either an abbreviation"
                        + " such as \"PST\", a full name such as \"America/Los_Angeles\", or a custom"
                        + " ID such as \"GMT-8:00\"GMT zone if the given ID is not understood")
                .setDefault(defaultTimeZone);
        subparser.addArgument("-n", "--server").action(Arguments.append())
                .metavar("SEVER_NAME")
                .help("exports the data for a particular server. When no argument is specified the data for all the servers is exported. Mulitple -n arguments are possible");
        return subparser;
    }

    private static Argument addDumpRootArg(Subparser exportDump) {
        return exportDump.addArgument("-r", "--dump-root")
                .metavar("PATH")
                .setDefault(DumpRootResolver.dumpRoot)
                .help("root folder to gather collected data from (default is execution-statistics-collector/dump)");
    }

    private static void configureLogger(Namespace ns) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        Logger root = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        int verbose = ns.getInt("verbose");
        root.setLevel(verbose == 0 ? Level.INFO : (verbose == 1 ? Level.DEBUG : Level.TRACE));
        root.detachAndStopAllAppenders();

        ConsoleAppender<ILoggingEvent> ca = new ConsoleAppender<ILoggingEvent>();
        ca.setContext(lc);
        ca.setName("console");
        PatternLayoutEncoder pl = new PatternLayoutEncoder();
        pl.setContext(lc);
        if (verbose == 0) {
            pl.setPattern("%d{HH:mm:ss.SSS} %-5level - %msg%n");
        } else {
            pl.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
        }
        pl.start();

        ca.setEncoder(pl);
        ca.start();
        root.addAppender(ca);
    }
}
