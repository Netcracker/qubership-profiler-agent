package com.netcracker.profiler.dump;

import com.netcracker.profiler.io.listener.FileRotatedListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class DumpFileManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DumpFileManager.class);

    private final FileRotatedListener fileRotatedListener = new FileRotatedListener() {
        public void fileRotated(DumpFile oldFile, DumpFile newFile) {
            if (oldFile == null) {
                return;
            }
            try {
                registerFile(oldFile, true);
            } catch (RuntimeException e) {
                log.warn("Can't register file {}", oldFile, e);
            }
        }
    };
    private final Map<String, DumpRoot> dumpRootsOrdered = new LinkedHashMap<String, DumpRoot>();
    private long maxAgeMillis, maxSize;
    private String root;
    private boolean initialized = false;
    private final FileDeleter fileDeleter = new FileDeleter();
    private DumpFileLog dumpFileLog;

    public DumpFileManager(long maxAgeMillis, long maxSize, String root) {
        this(maxAgeMillis, maxSize, root, false);
    }

    public DumpFileManager(long maxAgeMillis, long maxSize, String root, boolean forceScanFolder) {
        try {
            init(maxAgeMillis, maxSize, root, forceScanFolder);
        } catch (RuntimeException e) {
            log.error(String.format("Can't initialize DumpFileManager with given parameters (maxAgeMillis: %d, maxSize: %d, root directory: %s)",
                    new Object[]{maxAgeMillis, maxSize, root}), e);
        }
    }

    public synchronized void rescan() {
        //TODO MALO: move close() to init
        close();
        init(this.maxAgeMillis, this.maxSize, this.root, true);
    }

    private void init(long maxAgeMillis, long maxSize, String root, boolean forceScanFolder) {
        if (initialized) {
            throw new IllegalStateException("DumpFileManager is already initialized. Close before reinitializing");
        }
        log.info("Initialize DumpFileManager with maxSize = {}, maxAgeMillis = {}, root = {}", new Object[]{maxSize, maxAgeMillis, root});

        this.maxAgeMillis = maxAgeMillis;
        this.maxSize = maxSize;
        this.root = root;

        dumpRootsOrdered.clear();
        // read file list if exists
        File fileList = new File(root, DumpFileLog.DEFAULT_NAME);
        boolean fileListParsed = false;
        dumpFileLog = new DumpFileLog(fileList);
        if (!forceScanFolder) {
            try {
                Queue<DumpFile> queue = dumpFileLog.parseIfPresent();
                if (queue != null) {
                    log.info("Found dump files list in {}", fileList);
                    for (DumpFile dumpFile : queue) {
                        registerFile(dumpFile, false, false);
                    }
                    fileListParsed = true;
                }
            } catch (RuntimeException e) {
                log.warn(String.format("Can't parse file %s. It will be rewritten", fileList), e);
            }
        }

        if (!fileListParsed) {
            if (forceScanFolder) {
                log.info("DumpFileManager is forced to read dump directory");
            }
            log.info("Read dump root directory {}", root);
            dumpFileLog.cleanup(null, true);
            DumpFilesFinder dumpFilesFinder = new DumpFilesFinder();
            Queue<DumpFile> dumpFiles = dumpFilesFinder.search(this.root);
            for (DumpFile dumpFile : dumpFiles) {
                registerFile(dumpFile, false);
            }
        }

        tryPruneOldFiles();

        initialized = true;
    }

    public void registerFile(DumpFile file, boolean tryPrune) {
        registerFile(file, tryPrune, true);
    }

    public void registerFile(DumpFile file, boolean tryPrune, boolean writeLog) {
        addFileWithoutStoring(file);
        if (writeLog) {
            dumpFileLog.writeAddition(file);
        }

        log.info("Added info about dump file {}. Current size is {} bytes", file, getCurrentSize());
        if (tryPrune) {
            tryPruneOldFiles();
        }
    }

    public long getCurrentSize() {
        long res = 0L;

        for (DumpRoot dumpRoot : dumpRootsOrdered.values()) {
            res += dumpRoot.getSizeInBytes();
        }
        return res;
    }

    private synchronized void addFileWithoutStoring(DumpFile file) {
        DumpRoot dumpRoot = dumpRootsOrdered.get(file.getDumpRootName());
        if (dumpRoot == null) {
            // Next code should be successful because we can't create an instance of DumpFile with short path without dumpRoot
            File f = new File(file.getPath());
            File typeDir = f.getParentFile();
            File dumpRootDir = typeDir.getParentFile();
            String dumpRootPath = dumpRootDir.getPath();
            dumpRoot = new DumpRoot(dumpRootPath);
            dumpRootsOrdered.put(dumpRoot.getName(), dumpRoot);
        }
        dumpRoot.registerFile(file);
    }

    private DumpFile getFirstFile() {
        if (dumpRootsOrdered.isEmpty()) {
            return null;
        }
        for (DumpRoot dumpRoot : dumpRootsOrdered.values()) {
            DumpFile file = dumpRoot.getFirstFile();
            if (file != null) {
                return file;
            }
        }
        return null;
    }

    public synchronized void tryPruneOldFiles() {
        if (dumpRootsOrdered.isEmpty()) {
            log.info("No files registered. Nothing to delete on pruning. Will wait for the next rotating");
            return;
        }
        log.info("Try prune dump files. Max age: {}; max size: {}", maxAgeMillis, maxSize);
        // Try delete by modification date
        if (maxAgeMillis != 0L) {
            DumpFile elderFile = getFirstFile();
            /**
             * Note: on the peek firstly will be 'trace'/'calls'/'xml' files.
             * Even if they are younger than 'dictionary' or others.
             */
            long borderTimestamp = System.currentTimeMillis() - maxAgeMillis;
            while (elderFile != null && elderFile.getTimestamp() < borderTimestamp) {
                long deletedSize = deleteDumpFile(elderFile);
                elderFile = getFirstFile();
            }
        }
        // Try delete by size
        long currentSize = getCurrentSize();

        while (maxSize != 0L && currentSize > maxSize) {
            // TODO it should be possible to delete dir completely if there are no low priority files
            DumpFile fileToDelete = getFirstFile();
            if (fileToDelete == null) {
                log.error("There are no registered files. But current size should be {}. Try 'Rescan' button from the UI"
                        , currentSize);
                return;
            }
            long deletedSize = deleteDumpFile(fileToDelete);
            currentSize -= deletedSize;
        }

        log.info("Dump files size after pruning is {}", currentSize);
    }

    /**
     * Deletes the {@code file} completely from the corresponding {@link DumpRoot}
     * @param file {@link DumpFile} to be deleted
     * @return size of deleted files
     */
    private long deleteDumpFile(DumpFile file) {
        log.info("Deleting {}", file);
        synchronized (this) {
            DumpRoot dumpRoot = dumpRootsOrdered.get(file.getDumpRootName());

            long sizeBefore = dumpRoot.getSizeInBytes();
            Collection<DumpFile> deletedFiles = dumpRoot.deleteFile(file, fileDeleter);
            long sizeAfter = dumpRoot.getSizeInBytes();
            for (DumpFile deletedFile : deletedFiles) {
                dumpFileLog.writeDeletion(deletedFile);
            }

            if (dumpRoot.isEmpty()) {
                dumpRootsOrdered.remove(dumpRoot.getName());
            }
            return sizeBefore - sizeAfter;
        }
    }

    private boolean tryDeleteByModifiedDate(DumpFile file) {
        if(file.getDependentFile() != null) {
            return false;
        }
        if (0L == maxAgeMillis) {
            return false;
        }
        long borderModificationTime = System.currentTimeMillis() - maxAgeMillis;
        if (file.getTimestamp() < borderModificationTime) {

            log.info("File {} has modification time {} which is older than allowable {}. Will delete it",
                    file, file.getTimestamp(), borderModificationTime);
            boolean success = fileDeleter.deleteFile(file);
            dumpFileLog.writeDeletion(file);
            return success;
        }
        return false;
    }

    public FileRotatedListener getFileRotatedListener() {
        return fileRotatedListener;
    }

    public void close() {
        dumpFileLog.close();
        initialized = false;
    }
}
