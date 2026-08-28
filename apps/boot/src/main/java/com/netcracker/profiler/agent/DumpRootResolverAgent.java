package com.netcracker.profiler.agent;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.logging.Level;

public class DumpRootResolverAgent {
    public static final boolean VERBOSE = Boolean.getBoolean("execution-statistics-collector.verbose");
    private static final ESCLogger logger = ESCLogger.getLogger(DumpRootResolverAgent.class.getName(), (VERBOSE ? Level.FINE : ESCLogger.ESC_LOG_LEVEL));
    public static final String PROFILER_HOME;
    public static final String CONFIG_FILE;
    public static final String DUMP_ROOT;
    public static final String SERVER_NAME = ServerNameResolverAgent.SERVER_NAME;

    static {
        String configFile = PropertyFacadeBoot.getProperty("profiler.config", null);
        String dumpRoot = PropertyFacadeBoot.getProperty("profiler.dump", null);
        String dumpHome = PropertyFacadeBoot.getProperty("profiler.dump.home", null);
        String profilerHome = PropertyFacadeBoot.getProperty("profiler.home", null);

        if (profilerHome == null && configFile != null) {
            try {
                final File configXml = new File(configFile).getAbsoluteFile();
                final File configDir = configXml.getParentFile();
                final File profilerHomeDir = configDir.getParentFile();
                profilerHome = profilerHomeDir.getAbsolutePath();
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "Profiler: unable to resolve execution-statistics-collector.home from execution-statistics-collector.config " + configFile, t);
            }
        }

        if (profilerHome == null)
            profilerHome = resolveRootFromClass();

        if (profilerHome == null)
            profilerHome = "applications/execution-statistics-collector";

        if (configFile == null)
            configFile = profilerHome + File.separatorChar + "config" + File.separatorChar + "_config.xml";

        if (dumpRoot == null) {
            if (dumpHome == null) {
                File profilerHomeFile = new File(profilerHome).getAbsoluteFile();
                File applications = profilerHomeFile.getParentFile();
                File ncHome = applications.getParentFile();
                dumpRoot = ncHome.getAbsolutePath() + File.separatorChar + "execution-statistics-collector" + File.separatorChar + "dump";
            } else {
                dumpRoot = dumpHome;
            }
            if (ServerNameResolverAgent.SERVER_NAME.length() != 0)
                dumpRoot += File.separatorChar + ServerNameResolverAgent.SERVER_NAME;
        }

        logger.fine("Profiler: execution-statistics-collector.home == " + profilerHome + ", execution-statistics-collector.config == " + configFile + ", execution-statistics-collector.dump == " + dumpRoot);

        PROFILER_HOME = profilerHome;
        if (System.getProperty("profiler.home") == null) {
            System.setProperty("profiler.home", PROFILER_HOME);
        }

        CONFIG_FILE = configFile;
        DUMP_ROOT = dumpRoot;
    }

    private static String resolveRootFromClass() {
        String fileName = "/" + DumpRootResolverAgent.class.getName().replace('.', '/') + ".class";
        URL url = DumpRootResolverAgent.class.getResource(fileName);
        URI uri;
        try {
            uri = url.toURI();
        } catch (URISyntaxException e) {
            if (VERBOSE) {
                logger.log(Level.WARNING, "", e);
            }
            return null;
        }

        if (!uri.getScheme().equals("jar")) {
            logger.warning("Profiler: agent jar is loaded from unknown location " + uri);
            return null;
        }

        String path = uri.getSchemeSpecificPart();
        if (path == null || !path.startsWith("file:")) {
            logger.warning("Profiler: path to agent jar should start with 'file:'. Actual path is " + path);
            return null;
        }

        int jarEnd = path.lastIndexOf("!/");
        int jarStart = "file:".length();
        if (jarEnd == -1) {
            logger.warning("Profiler: path to agent jar should contain '!/'. Actual path is " + path);
            return null;
        }

        path = path.substring(jarStart, jarEnd);

        File f = new File(path).getParentFile();
        int i;
        for (i = 0; i < 5 && f != null && !new File(f, "config/_config.xml").exists(); i++) {
            f = f.getParentFile();
        }
        if (i == 5 || f == null) return null;
        if (VERBOSE) {
            logger.fine("Auto detected Profiler home folder: " + f.getAbsolutePath());
        }
        return f.getAbsolutePath();
    }
}
