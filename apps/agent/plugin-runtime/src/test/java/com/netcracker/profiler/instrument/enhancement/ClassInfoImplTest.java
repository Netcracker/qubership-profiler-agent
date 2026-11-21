package com.netcracker.profiler.instrument.enhancement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Each case gives the class the code source URL its class loader reports: {@code jar:nested:…/!…!/} for the Spring
 * Boot loader since 3.2, {@code jar:file:…!/…!/} for the loader before 3.2, and {@code file:…} for a plain jar.
 */
class ClassInfoImplTest {
    @TempDir
    Path tempDir;

    @Test
    void aJarNestedInABoot32FatJarReportsItsOwnManifest() throws IOException {
        Path fatJar = fatJarWithLib("BOOT-INF/lib/postgresql.jar", "42.7.14");

        assertEquals("42.7.14", implementationVersion(jarUrl("nested:" + fatJar.toUri().getPath() + "/!BOOT-INF/lib/postgresql.jar!/")));
    }

    @Test
    void aJarNestedInALegacyFatJarReportsItsOwnManifest() throws IOException {
        Path fatJar = fatJarWithLib("BOOT-INF/lib/postgresql.jar", "42.7.14");

        assertEquals("42.7.14", implementationVersion(jarUrl("file:" + fatJar.toUri().getPath() + "!/BOOT-INF/lib/postgresql.jar!/")));
    }

    @Test
    void aMissingNestedJarReportsUnknown() throws IOException {
        Path fatJar = fatJarWithLib("BOOT-INF/lib/postgresql.jar", "42.7.14");

        assertEquals("unknown", implementationVersion(jarUrl("nested:" + fatJar.toUri().getPath() + "/!BOOT-INF/lib/absent.jar!/")));
    }

    @Test
    void aPlainJarUnderADirectoryNamedWithAnExclamationMarkIsNotTakenForANestedOne() throws IOException {
        Path dir = Files.createDirectories(tempDir.resolve("!lib"));
        Path plainJar = dir.resolve("postgresql.jar");
        Files.write(plainJar, jarBytes("42.7.14"));

        assertEquals("42.7.14", implementationVersion(plainJar.toUri().toURL()));
    }

    private static String implementationVersion(URL codeSourceLocation) {
        ClassInfoImpl info = new ClassInfoImpl();
        info.setProtectionDomain(
                new ProtectionDomain(new CodeSource(codeSourceLocation, (Certificate[]) null), null));
        return info.getJarAttribute("Implementation-Version");
    }

    /**
     * Builds a {@code jar:} URL without the JDK handler, which refuses the {@code nested:} protocol that only the
     * Spring Boot loader registers.
     */
    private static URL jarUrl(String file) throws IOException {
        return new URL("jar", "", -1, file, new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                throw new UnsupportedOperationException(u.toString());
            }
        });
    }

    /**
     * Builds a fat jar whose own manifest says {@code 1.0.0-app} and which holds, at {@code libEntry}, a jar whose
     * manifest says {@code libVersion}, so a reader that returns the outer manifest fails the assertion.
     */
    private Path fatJarWithLib(String libEntry, String libVersion) throws IOException {
        Path fatJar = tempDir.resolve("app.jar");
        try (OutputStream out = Files.newOutputStream(fatJar);
             JarOutputStream jar = new JarOutputStream(out, manifest("1.0.0-app"))) {
            jar.putNextEntry(new JarEntry(libEntry));
            jar.write(jarBytes(libVersion));
            jar.closeEntry();
        }
        return fatJar;
    }

    private static byte[] jarBytes(String implementationVersion) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes, manifest(implementationVersion))) {
            jar.putNextEntry(new JarEntry("org/postgresql/Driver.class"));
            jar.closeEntry();
        }
        return bytes.toByteArray();
    }

    private static Manifest manifest(String implementationVersion) {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, implementationVersion);
        return manifest;
    }
}
