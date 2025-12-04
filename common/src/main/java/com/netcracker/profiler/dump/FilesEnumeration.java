package com.netcracker.profiler.dump;

import java.io.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class FilesEnumeration implements Enumeration<InputStream> {
    static final int DEFAULT_BUFFER_SIZE = 131072;
    Iterator<File> it;
    File currentFile;
    int bufferSize;

    public FilesEnumeration(Iterator<File> it) {
        this(it, DEFAULT_BUFFER_SIZE);
    }

    public FilesEnumeration(Iterator<File> it, int bufferSize) {
        this.it = it;
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean hasMoreElements() {
        return it.hasNext();
    }

    @Override
    public InputStream nextElement() {
        try {
            currentFile= it.next();
            return openInputStream(currentFile);
        } catch (EOFException e) {
            return new EOFInputStream();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long currentFileLenght(){
        return currentFile.length();
    }

    private InputStream openInputStream(File file) throws IOException {
        boolean isGzip = file.getName().endsWith(".gz");
        try {
            return new BufferedInputStream(new GZIPInputStream(new FileInputStream(file.getAbsolutePath() + (isGzip ? "" : ".gz")), bufferSize), bufferSize);
        } catch (FileNotFoundException e) {
            /* fall through -- will try find .gz file later */
        }
        String fileName = file.getAbsolutePath();
        if (isGzip) {
            fileName = fileName.substring(0, fileName.length() - 3);
        }
        return new BufferedInputStream(new FileInputStream(fileName), bufferSize);
    }

}
