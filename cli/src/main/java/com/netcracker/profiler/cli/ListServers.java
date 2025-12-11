package com.netcracker.profiler.cli;

import com.netcracker.profiler.guice.DumpRootLocation;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import jakarta.inject.Inject;

/**
 * Lists valid server names in the specified root
 */
public class ListServers implements Command {
    public static final Logger log = LoggerFactory.getLogger(ListServers.class);
    private final File dumpRoot;

    @Inject
    public ListServers(@DumpRootLocation File dumpRoot) {
        this.dumpRoot = dumpRoot;
    }

    public int accept(Namespace args) {
        if (dumpRoot == null) {
            log.warn("No dump path found - {}. Please check path to ESC dump (--dump-root)", dumpRoot);
            return -2;
        }
        File[] servers = dumpRoot.listFiles(File::isDirectory);
        if (servers == null || servers.length == 0) {
            log.warn("No servers found in {}. Please check path to ESC dump (--dump-root).", dumpRoot.getAbsolutePath());
            return -2;
        }
        for (File server : servers) {
            log.info("Server: {}", server.getName());
        }
        return 0;
    }
}
