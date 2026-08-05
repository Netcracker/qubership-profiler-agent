package com.netcracker.profiler.agent;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.jar.Attributes;

public class BootstrapTest {
    private static Attributes manifest(String... keysAndValues) {
        Attributes attrs = new Attributes();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            attrs.putValue(keysAndValues[i], keysAndValues[i + 1]);
        }
        return attrs;
    }

    private static Set<String> ids(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    @Test
    void extractPluginIds_fromExplicitAttribute() {
        assertEquals(
                ids("spring", "entry-points:com.example.SpringPlugin"),
                Bootstrap.extractPluginIds(
                        manifest("Plugin-Id", "spring", "Entry-Points", "com.example.SpringPlugin")));
    }

    @Test
    void extractPluginIds_fromEntryPoints() {
        assertEquals(
                ids(
                        "spring",
                        "entry-points:com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_spring"),
                Bootstrap.extractPluginIds(
                        manifest(
                                "Entry-Points",
                                "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_spring")));
    }

    @Test
    void extractPluginIds_explicitAndEntryPointsAreBothIdentities() {
        assertEquals(
                ids(
                        "explicit-id",
                        "fallback",
                        "entry-points:com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_fallback"),
                Bootstrap.extractPluginIds(
                        manifest(
                                "Plugin-Id", "explicit-id",
                                "Entry-Points",
                                "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_fallback")));
    }

    @Test
    void extractPluginIds_multipleEntryPoints() {
        assertEquals(
                ids(
                        "jdbc",
                        "entry-points:com.example.Other "
                                + "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_jdbc"),
                Bootstrap.extractPluginIds(
                        manifest(
                                "Entry-Points",
                                "com.example.Other "
                                        + "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_jdbc")));
    }

    @Test
    void extractPluginIds_multipleEnhancers() {
        assertEquals(
                ids(
                        "jdbc",
                        "spring",
                        "entry-points:com.example.Other "
                                + "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_jdbc "
                                + "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_spring"),
                Bootstrap.extractPluginIds(
                        manifest(
                                "Entry-Points",
                                "com.example.Other "
                                        + "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_jdbc "
                                        + "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_spring")));
    }

    /**
     * The runtime JAR predating {@code Plugin-Id} must land on the same identity as the release that
     * introduced the attribute, otherwise a stale copy loads next to the current one and every
     * enhancer fails to cast — see issue #412.
     */
    @Test
    void extractPluginIds_entryPointSetIsSharedAcrossReleases() {
        String entryPoints = "com.netcracker.profiler.agent.plugins.EnhancerRegistryPluginImpl "
                + "com.netcracker.profiler.agent.plugins.ProfilerTransformerPluginImpl "
                + "com.netcracker.profiler.agent.plugins.DumperPluginImpl";
        Set<String> withoutAttribute = Bootstrap.extractPluginIds(manifest("Entry-Points", entryPoints));
        Set<String> withAttribute =
                Bootstrap.extractPluginIds(manifest("Plugin-Id", "profiler-runtime", "Entry-Points", entryPoints));

        assertFalse(Collections.disjoint(withoutAttribute, withAttribute),
                "Expected a shared plugin identity between " + withoutAttribute + " and " + withAttribute);
    }

    @Test
    void extractPluginIds_entryPointOrderDoesNotMatter() {
        assertEquals(
                Bootstrap.extractPluginIds(manifest("Entry-Points", "com.example.A com.example.B")),
                Bootstrap.extractPluginIds(manifest("Entry-Points", "com.example.B com.example.A")));
    }

    @Test
    void extractPluginIds_noEntryPoints() {
        assertEquals(Collections.emptySet(), Bootstrap.extractPluginIds(manifest("Plugin-Id", "spring")));
    }

    @Test
    void extractPluginIds_nullAttributes() {
        assertEquals(Collections.emptySet(), Bootstrap.extractPluginIds(null));
    }

    @Test
    void extractPluginIds_emptyAttributes() {
        assertEquals(Collections.emptySet(), Bootstrap.extractPluginIds(new Attributes()));
    }

    @Test
    void compareVersions_numericSegments() {
        assertTrue(Bootstrap.compareVersions("4.0.5", "3.0.6") > 0);
        assertTrue(Bootstrap.compareVersions("3.0.6", "4.0.5") < 0);
        assertTrue(Bootstrap.compareVersions("4.0.10", "4.0.9") > 0);
        assertEquals(0, Bootstrap.compareVersions("4.0.5", "4.0.5"));
    }

    @Test
    void compareVersions_shorterVersionLosesToLongerNumericOne() {
        assertTrue(Bootstrap.compareVersions("4.0", "4.0.1") < 0);
        assertTrue(Bootstrap.compareVersions("4.0.1", "4.0") > 0);
    }

    @Test
    void compareVersions_qualifierLosesToTheRelease() {
        assertTrue(Bootstrap.compareVersions("4.0.5-SNAPSHOT", "4.0.5") < 0);
        assertTrue(Bootstrap.compareVersions("4.0.5", "4.0.5-SNAPSHOT") > 0);
    }

    @Test
    void compareVersions_missingVersionLosesToAnyVersion() {
        assertTrue(Bootstrap.compareVersions(null, "1.0") < 0);
        assertTrue(Bootstrap.compareVersions("1.0", null) > 0);
        assertEquals(0, Bootstrap.compareVersions(null, null));
    }
}
