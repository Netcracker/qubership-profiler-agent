package com.netcracker.profiler.instrument.enhancement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

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

        JarFile jarFile = null;
        try {
            if(fullJarName.contains("!/")) { //If it's a nested jar
                jarFile = ((JarURLConnection) new URL("jar:file:"+fullJarName+"!/").openConnection()).getJarFile();
            } else {
                jarFile = new JarFile(fullJarName);
            }
            jarManifest = jarFile.getManifest();
        } catch (IOException e) {
            //do not spam exceptions in logs with this one
            log.warn("Unable to open jar file {}", fullJarName);
            log.debug("Unable to open jar file", e);
        } finally {
            if (jarFile != null)
                try {
                    jarFile.close();
                } catch (IOException e) {
                    /**/
                }
        }

        return jarManifest;
    }
}
