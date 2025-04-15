package org.qubership.profiler;

import org.qubership.profiler.agent.ProfilingTransformer;
import org.qubership.profiler.agent.plugins.ConfigurationSPI;
import org.qubership.profiler.configuration.ConfigurationImpl;
import org.qubership.profiler.util.IOHelper;
import org.qubership.profiler.util.ZipUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;
import java.util.Enumeration;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.ParserConfigurationException;

public class Instrumenter {
    private final static Logger log = LoggerFactory.getLogger(Instrumenter.class);
    private final ConfigurationSPI conf;
    private ProfilingTransformer pt;

    public Instrumenter(ConfigurationSPI conf) {
        this.conf = conf;
        this.pt = new ProfilingTransformer(conf);
    }

    public void processJar(String fileName, File destination) throws IOException {
        log.info("Processing jar {}", fileName);
        long time = System.currentTimeMillis();
        ZipFile jar = new ZipFile(fileName);
        if (!archiveHasInterestingClasses(jar)) return;
        JarOutputStream out = null;
        int updated = 0, total = 0;

        try {
            if (destination != null) {
                final File outDir = destination.getParentFile();
                if (outDir.mkdirs())
                    log.info("Successfully created output directory {}", outDir.getAbsolutePath());
                out = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(destination)));
            }

            Enumeration<? extends ZipEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                String entryName = zipEntry.getName();
                byte[] byteCode = null;
                if (entryName.endsWith(".class")) {
                    total++;
                    final String className = fileNameToClassName(entryName);
                    if (pt.transformRequired(className)) {
                        try {
                            byteCode = pt.transform(null, className, null, null, IOHelper.readFully(jar.getInputStream(zipEntry)));
                        } catch (IOException e) {
                            log.warn("Unable to read from the stream. File {} will not be instrumented", entryName, e);
                        } catch (IllegalClassFormatException e) {
                            log.warn("Unable to transform zip entry {}", entryName, e);
                        }
                    }
                }

                if (out != null) {
                    if (byteCode == null) {
                        ZipUtils.copyEntry(out, jar, zipEntry);
                        continue;
                    }

                    ZipEntry ze = new ZipEntry(entryName);
                    ze.setComment(zipEntry.getComment());
                    ze.setExtra(zipEntry.getExtra());
                    ze.setMethod(zipEntry.getMethod());
                    ze.setTime(System.currentTimeMillis());
                    out.putNextEntry(ze);
                    out.write(byteCode, 0, byteCode.length);
                    out.closeEntry();
                }
                updated++;
            }
        } finally {
            time = System.currentTimeMillis() - time;
            log.info("Updated {} of {} classes in {} seconds in jar {}", new Object[]{updated, total, time / 1000.0, fileName});
            if (out != null) try {
                out.close();
            } catch (IOException e) {/**/}
            if (updated == 0 && destination != null) {
                if (!destination.delete())
                    log.info("Unable to delete file {}. The file does not contain modified classes, so there is no need to keep it", destination.getAbsolutePath());
            }
        }
    }

    private boolean archiveHasInterestingClasses(ZipFile zipFile) {
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry zipEntry = entries.nextElement();
            String entryName = zipEntry.getName();
            if (!entryName.endsWith(".class"))
                continue;
            entryName = fileNameToClassName(entryName);
            if (pt.transformRequired(entryName)) return true;
        }
        return false;
    }

    private static String fileNameToClassName(String entryName) {
        return entryName.substring(0, entryName.length() - 6 /* ".class".length() */);
    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        ConfigurationSPI conf = new ConfigurationImpl(args[0]);
        Instrumenter inst = new Instrumenter(conf);
        for (int i = 1; i < args.length; i++) {
            String jarName = args[i];
            File file = new File(jarName);
            if (file.isDirectory()) continue;
            inst.processJar(jarName, new File(jarName + ".patched"));
        }
    }
}
