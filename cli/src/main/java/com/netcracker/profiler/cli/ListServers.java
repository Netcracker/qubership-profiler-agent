package com.netcracker.profiler.cli;

import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.servlet.SpringBootInitializer;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;

/**
 * Lists valid server names in the specified root
 */
public class ListServers implements Command {
    public static final Logger log = LoggerFactory.getLogger(ListServers.class);

    protected final static FileFilter DIRECTORY_FILTER = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };

    protected void setupDumpRoot(Namespace args) {
        String dumpRoot = args.getString("dump_root");
        if (dumpRoot != null) {
            File root = new File(dumpRoot);
            if ("dump".equals(root.getName())) {
                dumpRoot += File.separatorChar + "default";
            } else if (new File(root, "dump").exists()) {
                dumpRoot += File.separatorChar + "dump" + File.separatorChar + "default";
            }
            DumpRootResolver.dumpRoot = dumpRoot;
        }
    }

    protected File getDumpRoot() {
        return new File(DumpRootResolver.dumpRoot).getParentFile();
    }

    public int accept(Namespace args) {
        setupDumpRoot(args);
        SpringBootInitializer.init();
        File dumpRoot = getDumpRoot();
        if (dumpRoot == null) {
            log.warn("No dump path found - {}. Please check path to ESC dump (--dump-root)", DumpRootResolver.dumpRoot);
            return -2;
        }
        File[] servers = dumpRoot.listFiles(DIRECTORY_FILTER);
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
