package com.netcracker.profiler.dump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

/**
 * {@link DumpFile} contains information about file path, its size and last modification time.<br/>
 *
 * @author logunov
 */
public class DumpFile {
    private static final Logger log = LoggerFactory.getLogger(DumpFile.class);
    public static final Collection<String> REMOVABLE_DIRS = Arrays.asList("trace", "xml", "calls", "sql");

    private final String path;
    private final String root;
    private final String parent;
    private final long size;
    private final long timestamp;

    private DumpFile dependentFile;

    /**
     * @param path file path
     * @param size file size
     * @param timestamp last modification time
     * @param dependentFile dump file can be deleted only after dependentFile is deleted
     * @throws NullPointerException if path is null or contains less than 3 levels of hierarchy
     */
    public DumpFile(String path, long size, long timestamp, DumpFile dependentFile) throws NullPointerException {
        if (path == null || path.length() == 0) {
            throw new NullPointerException("Parameter 'path' can't be null or empty");
        }
        this.path = path;
        // Path is like execution-statistics-collector/dump/u214_d93_6307Srv/2015/03/11/1426083986775/calls/000001.gz
        File parent = new File(path).getParentFile();
        String parentDirName = parent.getName();
        File grandparent = parent.getParentFile();
        String rootDirName = grandparent.getName();

        this.root = rootDirName;
        this.parent = parentDirName;
        this.size = size;
        this.timestamp = timestamp;
        this.dependentFile = dependentFile;
    }

    public DumpFile(String path, long size, long timestamp) throws NullPointerException {
        this(path, size, timestamp, null);
    }

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getRootDirName() {
        return root;
    }

    public String getParentDirName() {
        return parent;
    }

    public String getDumpRootName() {
        return root;
    }

    public DumpFile getDependentFile() {
        return dependentFile;
    }

    public void setDependentFile(DumpFile dependentFile) {
        this.dependentFile = dependentFile;
    }

    public boolean delete() {
        File f = new File(path);
        log.info("Deleting file {}", f);
        return f.delete();
    }

    @Override
    public String toString() {
        return "DumpFile{" +
            "path='" + path + '\'' +
            ", size=" + size +
            ", timestamp=" + timestamp +
            ", dependentFile=" + dependentFile +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DumpFile dumpFile = (DumpFile) o;

        if (!path.equals(dumpFile.path)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
