package com.netcracker.profiler.dump;

import org.apache.commons.lang.math.NumberUtils;

import java.io.File;
import java.io.FileFilter;
import java.util.*;

public class DumpFilesFinder {

    private static final FileFilter DIRECTORY_FINDER = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };
    private static final Comparator<File> ORDER_BY_LAST_MODIFIED_ASC = new Comparator<File>() {
        public int compare(File a, File b) {
            long compareResult = a.lastModified() - b.lastModified();
            return compareResult == 0
                    ? 0
                    : (compareResult > 0 ? 1 : -1);
        }
    };

    public Queue<DumpFile> search(String path) {
        if (path == null) {
            return null;
        }
        Queue<DumpFile> queue = new LinkedList<DumpFile>();
        findInFolder(new File(path), queue, 0);
        return queue;
    }


    private void findInFolder(File root, Queue<DumpFile> queueToFill, int level) {
        if (level == 4) {
            /* We are at root/2010/04/24/123342342 */

            ArrayList<File> files = new ArrayList<File>();
            ArrayList<File> sqlFiles = new ArrayList<File>();

            for(File child : root.listFiles()) {
                if("sql".equals(child.getName())) {
                    addFiles(sqlFiles, child);
                } else {
                    addFiles(files, child);
                }
                deleteFolderIfEmpty(child);
            }

            Collections.sort(files, ORDER_BY_LAST_MODIFIED_ASC);
            Collections.sort(sqlFiles, ORDER_BY_LAST_MODIFIED_ASC);
            files.addAll(sqlFiles); //SQL Files should be placed at the end of the queue to be removed only after all currently existing trace files

            for (File file : files) {
                DumpFile dumpFile = new DumpFile(file.getPath(), file.length(), file.lastModified());
                queueToFill.add(dumpFile);
            }
            deleteFolderIfEmpty(root);
            return;
        }
        if (root.isDirectory()) {
            if(level == 1 && !NumberUtils.isNumber(root.getName())) {
                return;
            }
            File[] files = root.listFiles(DIRECTORY_FINDER);
            if (files.length == 0) {
                root.delete();
                return;
            }
            Arrays.sort(files);
            for (File f : files) {
                findInFolder(f, queueToFill, level + 1);
            }
            deleteFolderIfEmpty(root);
        }
    }

    private void addFiles(ArrayList<File> files, File folder) {
        files.addAll(Arrays.asList(folder.listFiles()));
    }

    private boolean deleteFolderIfEmpty(File folder) {
        if(folder.listFiles().length == 0) {
            return folder.delete();
        }
        return false;
    }

}
