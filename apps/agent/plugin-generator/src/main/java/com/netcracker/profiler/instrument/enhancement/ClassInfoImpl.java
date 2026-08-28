package com.netcracker.profiler.instrument.enhancement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
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

        int jarSeparatorIdx =  fullJarName.indexOf("!/");
        if(jarSeparatorIdx == -1) {
            jarSeparatorIdx =  fullJarName.indexOf("/!");
        }

        try {
            if(jarSeparatorIdx == -1) {
                try (JarFile jarFile = new JarFile(fullJarName)) {
                    jarManifest = jarFile.getManifest();
                }
            } else {//If it's a nested jar
                String outerJarPath = fullJarName.substring(0, jarSeparatorIdx);
                String nestedJarPath = fullJarName.substring(jarSeparatorIdx+2);
                jarManifest = readManifestFromNestedJar(outerJarPath, nestedJarPath);
            }
        } catch (IOException e) {
            //do not spam exceptions in logs with this one
            log.warn("Unable to open jar file {}. Message {}", fullJarName, e.getMessage());
            log.debug("Unable to open jar file", e);
        }

        return jarManifest;
    }

    private Manifest readManifestFromNestedJar(String outerJarPath, String nestedJarPath) throws IOException {
        try (JarFile outerJar = new JarFile(outerJarPath)) {
            JarEntry nestedJarEntry = outerJar.getJarEntry(nestedJarPath);
            if (nestedJarEntry == null) {
                throw new IOException("Nested JAR not found: " + nestedJarPath);
            }

            try (InputStream nestedJarStream = outerJar.getInputStream(nestedJarEntry);
                 ZipInputStream zipInputStream = new ZipInputStream(nestedJarStream)) {

                ZipEntry entry;
                while ((entry = zipInputStream.getNextEntry()) != null) {
                    if ("META-INF/MANIFEST.MF".equalsIgnoreCase(entry.getName())) {
                        return new Manifest(zipInputStream);
                    }
                }
            }
        }

        throw new IOException("Manifest not found in nested JAR: " + nestedJarPath);
    }
}
