package com.netcracker.profiler.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileWalker {
    public static final Logger log = LoggerFactory.getLogger(FileWalker.class);
    private final InputStreamProcessor out;

    public FileWalker(InputStreamProcessor out) {
        this.out = out;
    }

    public void walk(String root) throws IOException {
        File f = new File(root);
        walkFileOrDirectory(f, f.isDirectory() ? new HashSet<String>() : Collections.<String>emptySet());
    }

    private boolean walkFileOrDirectory(File f, Set<String> processedFiles) throws IOException {
        String path = f.getAbsolutePath();
        if (f.isDirectory())
            log.info("Looking in directory {} for thread dumps", path);
        else
            log.info("Looking in file {} for thread dumps, file size is {}", path, f.length());

        if (f.isDirectory()) {
            if (!processedFiles.add(path)) {
                log.info("Looks like directory {} is recursively included into itself", path);
                return true; // recursive subfolders detected
            }

            for (File file : f.listFiles())
                if (!walkFileOrDirectory(file, processedFiles))
                    break;
            return true;
        }

        FileInputStream is = null;
        try {
            is = new FileInputStream(f);
            return walkFileOrArchive(f.getName(), is);
        } finally {
            if (is != null) is.close();
        }
    }

    private boolean walkFileOrArchive(String fileName, InputStream is) throws IOException {
        if (fileName.endsWith(".gz")) {
            log.info("Processing gzip archive {}", fileName);
            GZIPInputStream gzip = null;
            try {
                gzip = new GZIPInputStream(new InputStreamNoCloseAdapter(is));
                return walkFileOrArchive(fileName.substring(0, fileName.length() - 2), gzip);
            } finally {
                if (gzip != null) gzip.close();
            }
        }
        if (fileName.endsWith(".zip")) {
            log.info("Processing zip archive {}", fileName);
            ZipInputStream zis = null;
            try {
                zis = new ZipInputStream(new InputStreamNoCloseAdapter(is));
                ZipEntry ze;
                while ((ze = zis.getNextEntry()) != null) {
                    if (ze.isDirectory() || ze.getSize() < 100) continue;
                    log.info("Processing zip file {}, size {}", ze.getName(), ze.getSize());
                    if (!walkFileOrArchive(ze.getName(), zis)) return false;
                }
            } finally {
                if (zis != null) zis.close();
            }
            return true;
        }

        return out.process(is, fileName);
    }
}
