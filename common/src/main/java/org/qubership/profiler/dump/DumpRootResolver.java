package org.qubership.profiler.dump;

import org.qubership.profiler.ServerNameResolver;
import org.qubership.profiler.configuration.PropertyFacade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;

public class DumpRootResolver {
    public static final boolean VERBOSE = Boolean.getBoolean("execution-statistics-collector.verbose");
    public static final String PROFILER_HOME;
    public static final String CONFIG_FILE;

    //It can be overrided by cli arg in standalone profiler UI
    public static String dumpRoot;
    private static final Logger logger = LoggerFactory.getLogger(DumpRootResolver.class);

    static {
        String configFile = PropertyFacade.getProperty("profiler.config", null);
        String dumpRoot = PropertyFacade.getProperty("profiler.dump", null);
        String dumpHome = PropertyFacade.getProperty("profiler.dump.home", null);
        String profilerHome = PropertyFacade.getProperty("profiler.home", null);

        if (profilerHome == null && configFile != null) {
            try {
                final File configXml = new File(configFile).getAbsoluteFile();
                final File configDir = configXml.getParentFile();
                final File profilerHomeDir = configDir.getParentFile();
                profilerHome = profilerHomeDir.getAbsolutePath();
            } catch (Throwable t) {
                log(false, "Profiler: unable to resolve execution-statistics-collector.home from execution-statistics-collector.config " + configFile, t);
            }
        }

        if (profilerHome == null)
            profilerHome = resolveRootFromClass();

        if (profilerHome == null)
            profilerHome = "applications/execution-statistics-collector";

        if (configFile == null)
            configFile = profilerHome + File.separatorChar + "config" + File.separatorChar + "_config.xml";

        if (dumpRoot == null) {
            if(dumpHome == null) {
                File profilerHomeFile = new File(profilerHome).getAbsoluteFile();
                File applications = profilerHomeFile.getParentFile();
                File ncHome = applications.getParentFile();
                dumpRoot = getProfilerHomePath(ncHome, "execution-statistics-collector");
                if (!new File(dumpRoot).exists()) { // fallback to "profiler' folder
                    String oldDumpRoot = dumpRoot;
                    dumpRoot = getProfilerHomePath(ncHome, "profiler");
                    log(true, "Profiler: dump root folder " + oldDumpRoot + " does not exists, will try " + dumpRoot);

                    if (!new File(dumpRoot).exists()) {
                        dumpRoot = oldDumpRoot;
                    }
                }
            } else {
                dumpRoot = dumpHome;
            }
            if (ServerNameResolver.SERVER_NAME.length() != 0)
                dumpRoot += File.separatorChar + ServerNameResolver.SERVER_NAME;
        }

        if (dumpRoot == null)
            log(true, "Profiler: unable to resolve execution-statistics-collector home folder.\n" +
                    "Please, specify property execution-statistics-collector.home (a folder that has config/_config.xml) or setup execution-statistics-collector.config (path to config file) & execution-statistics-collector.dump properties explicitly");

        try {
            Class.forName("org.qubership.profiler.agent.Bootstrap");
            // javaagent is present, skip printing homes
        } catch (ClassNotFoundException e) {
            // javaagent is missing, print home path
            log(true, "Profiler: execution-statistics-collector.home == " + profilerHome + ", execution-statistics-collector.config == " + configFile + ", execution-statistics-collector.dump == " + dumpRoot);
        }

        PROFILER_HOME = profilerHome;
        if (System.getProperty("profiler.home") == null) {
            System.setProperty("profiler.home", PROFILER_HOME);
        }

        CONFIG_FILE = configFile;
        DumpRootResolver.dumpRoot = dumpRoot;
    }

    private static String getProfilerHomePath(File ncHome, String profilerFolderName) {
        return ncHome.getAbsolutePath() + File.separatorChar + profilerFolderName + File.separatorChar + "dump";
    }

    private static String resolveRootFromClass() {
        final ProtectionDomain pd = DumpRootResolver.class.getProtectionDomain();
        if (pd == null) return null;
        final CodeSource source = pd.getCodeSource();
        if (source == null) return null;
        final URL location = source.getLocation();
        if (location == null) return null;
        File f = new File(location.getFile()).getParentFile();
        int i;
        for (i = 0; i < 5 && f != null && !new File(f, "config/_config.xml").exists(); i++) {
            f = f.getParentFile();
        }
        if (i == 5 || f == null) return null;
        if (VERBOSE) {
            log(true, "Auto detected Profiler home folder: " + f.getAbsolutePath());
        }
        return f.getAbsolutePath();
    }

    private static void log(boolean verbose, String message){
        log(verbose, message, null);
    }

    private static void log(boolean verbose, String message, Throwable t){
        if (verbose && !VERBOSE) {
            return;
        }
        if(t != null) {
            logger.error(message, t);
        } else {
            logger.info(message);
        }
    }
}
