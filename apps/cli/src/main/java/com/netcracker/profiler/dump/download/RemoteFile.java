package com.netcracker.profiler.dump.download;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RemoteFile implements Comparable<RemoteFile> {
    public static final char separatorChar = '/';
    String path;
    RemoteFileManager remoteFileManager;
    Boolean isDirectory;
    Boolean isFile;
    Long lastModificationTime;
    Long length;
    Boolean fileExists;
    List<RemoteFile> childFiles;

    public RemoteFile(String path, RemoteFileManager remoteFileManager) {
        this.path = path;
        this.remoteFileManager = remoteFileManager;
    }

    public String getPath() {
        return path;
    }

    public String getShellQuotedPath() {
        return "'" + path.replace("'", "'\\''") + "'";
    }

    public String getName() {
        int index = path.lastIndexOf(separatorChar);
        if (index <= 0) {
            return path;
        }
        return path.substring(index + 1);
    }

    public RemoteFile childFile(String fileName) {
        return childFile(fileName, false);
    }

    public RemoteFile childFile(String fileName, boolean fromCache) {
        return remoteFileManager.getFile(getPath() + separatorChar + fileName, fromCache);
    }

    public List<RemoteFile> listFiles() throws IOException {
        return listFiles((FileFilter) null);
    }

    public List<RemoteFile> listFiles(FileFilter filter) throws IOException {
        if (this.childFiles == null) {
            List<RemoteFile> files = remoteFileManager.listFiles(this);
            if (files == null) {
                return Collections.emptyList();
            }
            if (filter == null) {
                childFiles = files;
            } else {
                List<RemoteFile> filteredFiles = new ArrayList<>();
                for (RemoteFile file : files) {
                    if (filter.accept(file)) {
                        filteredFiles.add(file);
                    }
                }
                childFiles = filteredFiles;
            }
        }
        return childFiles;
    }

    public String getParent() {
        int index = path.lastIndexOf(separatorChar);
        if (index <= 0) {
            return null;
        }
        return path.substring(0, index);
    }

    public RemoteFile getParentFile() {
        return remoteFileManager.getFile(getParent());
    }


    public boolean exists() throws IOException {
        if (fileExists == null) {
            readAttributes();
        }
        return fileExists;
    }


    public boolean isDirectory() throws IOException {
        if (isDirectory == null) {
            readAttributes();
        }
        return isDirectory;
    }


    public boolean isFile() throws IOException {
        if (isFile) {
            readAttributes();
        }
        return isFile;
    }


    public long lastModified() throws IOException {
        if (lastModificationTime == null) {
            readAttributes();
        }
        return lastModificationTime;
    }

    public long length() throws IOException {
        if (length == null) {
            readAttributes();
        }
        return length;
    }

    private void readAttributes() throws IOException {
        remoteFileManager.readAttrs(this);
    }

    @Override
    public String toString() {
        return getPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteFile that = (RemoteFile) o;

        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public int compareTo(RemoteFile o) {
        return path.compareTo(o.path);
    }

    public interface FileFilter {
        boolean accept(RemoteFile pathname);
    }
}
