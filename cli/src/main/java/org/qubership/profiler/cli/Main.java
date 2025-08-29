package org.qubership.profiler.cli;

import org.qubership.profiler.dump.DumpRootResolver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.*;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

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
        ArgumentParserBuilder parserBuilder = ArgumentParsers.newFor("esc-cmd.sh");
        if (System.getProperty("java.specification.version").startsWith("1.5")) {
            // Does not work due to usage of File.canExecute
            parserBuilder.terminalWidthDetection(false);
        }
        ArgumentParser parser = parserBuilder.build();
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
                .setDefault(COMMAND_ID, new ListServers());
        addDumpRootArg(listServers);

        Subparser exportDump = subparsers
                .addParser("export-dump")
                .help("Export the collected data for the specified time-frame to a separate archive");
        exportDump.addArgument("-d", "--dry-run").action(Arguments.storeTrue())
                .help("skips export, just scans the folders and prints the estimated size of the archive");
        exportDump.addArgument("-q", "--skip-details").action(Arguments.storeTrue())
                .help("exports only high-level information, skips export of profiling trees");
        addExportArgs(exportDump, new ExportDump(), ExportDump.DEFAULT_FILE_NAME);

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
        addExportArgs(exportExcel, new ExportExcel(), ExportExcel.DEFAULT_FILE_NAME);

        if (args.length == 0) {
            parser.printHelp();
            return;
        }

        Namespace ns = parser.parseArgsOrFail(args);

        configureLogger(ns);

        Command cmd = ns.get(COMMAND_ID);
        int code = cmd.accept(ns);
        if (code != 0) {
            System.exit(code);
        }
    }

    private static Subparser addExportArgs(Subparser subparser, Command command, String defaultOutputFileName) {
        subparser = subparser
                .defaultHelp(true)
                .epilog(DATE_FORMATS_EPILOG);
        subparser.setDefault(COMMAND_ID, command);
        subparser.addArgument("output_file").metavar("OUTPUT_FILE").help("specifies the name of the output file")
                .nargs("?")
                .setDefault(defaultOutputFileName);
        subparser.addArgument("-s", "--start-date").metavar("DATE").help("specifies the start timestamp of the export time-frame")
                .setDefault("1hour");
        subparser.addArgument("-e", "--end-date").metavar("DATE").help("specifies the end timestamp of the export time-frame")
                .setDefault("now");
        subparser.addArgument("-z", "--time-zone").metavar("TIME_ZONE").help("specifies time zone to disambiguate timestamp arguments. either an abbreviation"
                        + " such as \"PST\", a full name such as \"America/Los_Angeles\", or a custom"
                        + " ID such as \"GMT-8:00\"GMT zone if the given ID is not understood")
                .setDefault(TimeZone.getDefault().getID());
        subparser.addArgument("-n", "--server").action(Arguments.append())
                .metavar("SEVER_NAME")
                .help("exports the data for a particular server. When no argument is specified the data for all the servers is exported. Mulitple -n arguments are possible");
        addDumpRootArg(subparser);
        return subparser;
    }

    private static Argument addDumpRootArg(Subparser exportDump) {
        return exportDump.addArgument("-r", "--dump-root").metavar("PATH")
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
