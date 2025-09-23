package com.netcracker.profiler.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipUtils {
    public static void copyEntry(ZipOutputStream out, ZipFile src, ZipEntry srcEntry) throws IOException {
        copyEntry(out, src, srcEntry, true);
    }

    public static void copyEntry(ZipOutputStream out, ZipFile src, ZipEntry srcEntry, boolean putNextEntry) throws IOException {
        byte[] buffer = new byte[8192];
        ZipEntry entry = src.getEntry(srcEntry.getName());
        entry.setCompressedSize(-1);
        if (putNextEntry)
            out.putNextEntry(entry);
        InputStream is = src.getInputStream(entry);
        for (int read; (read = is.read(buffer)) > 0; out.write(buffer, 0, read)) ;
        if (putNextEntry)
            out.closeEntry();
    }

}
