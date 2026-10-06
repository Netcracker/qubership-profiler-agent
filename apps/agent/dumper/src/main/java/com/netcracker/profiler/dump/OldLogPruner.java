package com.netcracker.profiler.dump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class OldLogPruner implements Runnable {
    public static final Logger log = LoggerFactory.getLogger(OldLogPruner.class);

    long maxFileTimestamp, maxSize;
    long size;
    File root;

    public OldLogPruner(long maxAge, long maxSize, File root) {
        this.maxFileTimestamp = System.currentTimeMillis() - maxAge;
        this.maxSize = maxSize;
        this.root = root;
    }

    public void run() {
        findInFolder(root, 0);
        log.info("Total space used by profiler log files after purge is {} Mb", size/1024/1024);
    }

    private void findInFolder(File root, int level) {
        if (level == 4) {
            /* We are at root/2010/04/24/123342342 */
            ArrayList<File> files = new ArrayList<File>();
            addFiles(files, root, "trace");
            addFiles(files, root, "xml");
            addFiles(files, root, "sql");

            Collections.sort(files, ORDER_BY_LAST_MODIFIED_DESC);

            for (File file : files) {
                if (size <= maxSize && file.lastModified() >= maxFileTimestamp)
                    size += file.length();
                else
                    deleteFile(file);
            }

            if (!files.isEmpty()) {
                files.clear();
                addFiles(files, root, "trace");
            }

            if (files.isEmpty())
                deleteFile(root);
            return;
        }
        if (root.isDirectory()) {
            final File[] files = root.listFiles(DIRECTORY_FINDER);
            Arrays.sort(files, Collections.reverseOrder());
            for (File f : files) {
                if (size <= maxSize)
                    findInFolder(f, level + 1);
                else
                    deleteFile(f);
            }
        }
    }

    final static FileFilter DIRECTORY_FINDER = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };

    final static Comparator<File> ORDER_BY_LAST_MODIFIED_DESC = new Comparator<File>() {
        public int compare(File a, File b) {
            long x = a.lastModified(), y = b.lastModified();
            if (x < y) return 1;
            if (x > y) return -1;
            return 0;
        }
    };

    private void addFiles(ArrayList<File> files, File root, String subfolder) {
        File callsFolder = new File(root, subfolder);
        if (!callsFolder.exists()) return;
        files.addAll(Arrays.asList(callsFolder.listFiles()));
    }

    static public boolean deleteFile(File path) {
        log.info("Deleting {} {}", path.isDirectory() ? "directory with subdirectories" : "file", path);
        if (path.exists() && path.isDirectory())
            deleteSubDirectories(path);
        return path.delete();
    }

    static public boolean deleteSubDirectories(File path) {
        if (path.exists() && path.isDirectory())
            for (File file : path.listFiles())
                deleteSubDirectories(file);
        return path.delete();
    }
}
