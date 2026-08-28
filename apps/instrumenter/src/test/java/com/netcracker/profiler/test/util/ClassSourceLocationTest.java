package com.netcracker.profiler.test.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.netcracker.profiler.instrument.TypeUtils;

import gnu.trove.impl.hash.THash;
import org.junit.jupiter.api.Test;

import java.io.File;

public class ClassSourceLocationTest {
    @Test
    public void classLoadedFromDirectory(){
        String path = TypeUtils.getJarName(getClass().getProtectionDomain());
        assertEquals("java/test/", path); // build/java/test/
    }

    @Test
    public void classLoadedFromJar(){
        String path = TypeUtils.getJarName(THash.class.getProtectionDomain());
        assertNotNull(path, "TypeUtils.getJarName(THash.class.getProtectionDomain()) should not be null");
        assertTrue(path.matches("[^/]+/[^/]+/trove4j.*.jar"), path + " does not match regex [^/]+/[^/]+/trove4j.*.jar");
    }

    @Test
    public void classLoadedFromJDK(){
        String path = TypeUtils.getJarName(Object.class.getProtectionDomain());
        assertNull(path);
    }

    private void testResolveMvn(String mvnUrl, String expectedJarName) {
        assertEquals(TypeUtils.resolveMvnJar(mvnUrl).replace(File.separatorChar, '/'), expectedJarName);
    }

    @Test
    public void testMvnJar() {
        testResolveMvn("mvn:com.oracle/ojdbc6/11.2.0.3.0-osgi", "system/com/oracle/ojdbc6/11.2.0.3.0-osgi/ojdbc6-11.2.0.3.0-osgi.jar");
    }

    @Test
    public void testWrapMvnJar() {
        testResolveMvn("wrap:mvn:org.infinispan/infinispan-core/5.2.6.Final$overwrite=merge", "system/org/infinispan/infinispan-core/5.2.6.Final/infinispan-core-5.2.6.Final.jar");
    }

    @Test
    public void testWrapMvnWithParamsJar() {
        testResolveMvn("wrap:mvn:org.infinispan/infinispan-core/5.2.6.Final$overwrite=merge&Import-Package=sun.misc;net.jcip.annotations;resolution:=optional,org.jgroups.*;version=\"[3.2,4)\",org.jboss.logging;version=\"[3.1,4)\",*&Export-Package=org.infinispan.*;-noimport:=true", "system/org/infinispan/infinispan-core/5.2.6.Final/infinispan-core-5.2.6.Final.jar");
    }
}
