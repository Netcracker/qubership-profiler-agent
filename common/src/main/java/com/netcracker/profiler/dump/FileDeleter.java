package com.netcracker.profiler.dump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class FileDeleter {
    private static final Logger log = LoggerFactory.getLogger(FileDeleter.class);
    private static final int MAX_DOWN_HIERARCHY_LEVEL = 3;
    private static final int MAX_UP_HIERARCHY_LEVEL = 3;

    public boolean deleteFile(DumpFile dumpFile) {
        log.info("Delete file {}", dumpFile);
        return deleteFile("file", new File(dumpFile.getPath()));
    }

    public boolean deleteRecursively(String path) {
        File dirFile = new File(path);
        if(!deleteDir(dirFile, 1)) {
            return false;
        }
        deleteParentIfEmpty(dirFile.getParentFile(), 1);
        return true;
    }

    private boolean deleteDir(File dir, int level) {
        if (dir.isFile()) {
            return deleteFile("file", dir);
        }
        if (level > MAX_DOWN_HIERARCHY_LEVEL) {
            log.warn("Skip deleting dir {}. Hierarchy limit {} exceeded.", dir, level);
            return false;
        }
        if (!dir.exists()) {
            log.warn("Directory {} does not exist", dir);
            return false;
        }
        if (!dir.isDirectory()) {
            log.warn("The path is expected to be a directory {}", dir);
            return false;
        }
        log.info("Removing directory {}", dir);
        File[] files = dir.listFiles();
        boolean childrenDeleted = true;
        if (files != null) {
            for (File file : files) {
                if (!deleteDir(file, level + 1)) {
                    childrenDeleted = false;
                }
            }
        }

        return childrenDeleted && deleteFile("directory", dir);
    }

    private boolean deleteFile(String title, File dir) {
        if (!dir.exists()) {
            log.warn("{} does not exist: {}", title, dir);
            return false;
        }
        final boolean outcome = dir.delete();
        if (outcome) {
            log.info("Removing {} {}", title, dir);
        } else {
            log.warn("Unable to remove {} {}", title, dir);
        }
        return outcome;
    }

    private boolean deleteParentIfEmpty(File parentFile, int level) {
        if (level > MAX_UP_HIERARCHY_LEVEL) {
            log.warn("Stop recursive deletion of empty dirs on dir {}. Max hierarchy level exceeded border value {}", parentFile, level);
            return false;
        }

        File[] files = parentFile.listFiles();
        if (files.length > 0) {
            log.info("Dir {} is not empty. Skip deleting", parentFile);
            return false;
        }

        boolean deleted = parentFile.delete();
        if (!deleted) {
            log.warn("Can't delete dir {}", parentFile);
            return false;
        }

        deleteParentIfEmpty(parentFile.getParentFile(), level + 1);

        return true;
    }
}
