package com.netcracker.profiler.dump;

import java.io.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class FilesEnumeration implements Enumeration<InputStream> {
    Iterator<File> it;
    File currentFile;

    public FilesEnumeration(Iterator<File> it) {
        this.it = it;
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

    private static InputStream openInputStream(File file) throws IOException {
        boolean isGzip = file.getName().endsWith(".gz");
        try {
            return new BufferedInputStream(new GZIPInputStream(new FileInputStream(file.getAbsolutePath() + (isGzip ? "" : ".gz")), 131072), 131072);
        } catch (FileNotFoundException e) {
            /* fall through -- will try find .gz file later */
        }
        String fileName = file.getAbsolutePath();
        if (isGzip)
            fileName = fileName.substring(0, fileName.length() - 2);
        return new BufferedInputStream(new FileInputStream(fileName), 131072);
    }

}
