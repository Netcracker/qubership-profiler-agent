package com.netcracker.profiler.instrument.enhancement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ClassInfoImpl extends ClassInfo {
    private static final Logger log = LoggerFactory.getLogger(ClassInfoImpl.class);
    private Manifest jarManifest;

    @Override
    public String getJarAttribute(String name) {
        Manifest manifest = getJarManifest();
        if(manifest == null) {
            return "unknown";
        }
        return jarManifest.getMainAttributes().getValue(name);
    }

    @Override
    public String getJarSubAttribute(String entryName, String attrName) {
        Manifest manifest = getJarManifest();
        if(manifest == null) return "unknown";

        Attributes attr = manifest.getAttributes(entryName);
        if(attr == null) return "unknown";

        return attr.getValue(attrName);
    }

    private Manifest getJarManifest() {
        if(jarManifest != null) {
            return jarManifest;
        }

        String fullJarName = getJarName();

        if (fullJarName == null) {
            log.info("Unable to get attribute for class {} since jar name is not known for protection domain {}", getClassName(), getProtectionDomain());
            return null;
        }

        try {
            jarManifest = readManifest(fullJarName);
        } catch (IOException e) {
            //do not spam exceptions in logs with this one
            log.warn("Unable to open jar file {}", fullJarName);
            log.debug("Unable to open jar file", e);
        }

        return jarManifest;
    }

    /**
     * Reads the manifest of a jar named the way {@code TypeUtils.getFullJarName} names it, or returns {@code null}
     * when the jar has no manifest.
     *
     * <p>A jar nested in another one is named {@code outer.jar!/BOOT-INF/lib/x.jar} by the Spring Boot loader before
     * 3.2 and {@code outer.jar/!BOOT-INF/lib/x.jar} by the loader since 3.2. Either name is read from the outer jar
     * when the part before the separator is a file; any other name is opened as a plain jar.
     */
    static Manifest readManifest(String fullJarName) throws IOException {
        int separator = fullJarName.indexOf("!/");
        if (separator == -1) {
            separator = fullJarName.indexOf("/!");
        }
        if (separator == -1 || !new File(fullJarName.substring(0, separator)).isFile()) {
            try (JarFile jarFile = new JarFile(fullJarName)) {
                return jarFile.getManifest();
            }
        }
        String outerJarName = fullJarName.substring(0, separator);
        String nestedJarName = fullJarName.substring(separator + 2);
        try (JarFile outerJar = new JarFile(outerJarName)) {
            JarEntry nestedJar = outerJar.getJarEntry(nestedJarName);
            if (nestedJar == null) {
                throw new FileNotFoundException(nestedJarName + " is not found in " + outerJarName);
            }
            try (ZipInputStream nestedJarStream = new ZipInputStream(outerJar.getInputStream(nestedJar))) {
                for (ZipEntry entry; (entry = nestedJarStream.getNextEntry()) != null; ) {
                    if (JarFile.MANIFEST_NAME.equalsIgnoreCase(entry.getName())) {
                        return new Manifest(nestedJarStream);
                    }
                }
            }
        }
        return null;
    }
}
