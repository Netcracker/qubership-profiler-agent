package com.netcracker.profiler.agent;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.jar.Attributes;

public class BootstrapTest {
    @Test
    void extractPluginId_fromExplicitAttribute() {
        Attributes attrs = new Attributes();
        attrs.putValue("Plugin-Id", "spring");
        assertEquals(Collections.singletonList("spring"), Bootstrap.extractPluginId(attrs));
    }

    @Test
    void extractPluginId_fromEntryPoints() {
        Attributes attrs = new Attributes();
        attrs.putValue("Entry-Points", "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_spring");
        assertEquals(Collections.singletonList("spring"), Bootstrap.extractPluginId(attrs));
    }

    @Test
    void extractPluginId_preferExplicitOverEntryPoints() {
        Attributes attrs = new Attributes();
        attrs.putValue("Plugin-Id", "explicit-id");
        attrs.putValue("Entry-Points", "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_fallback");
        assertEquals(Collections.singletonList("explicit-id"), Bootstrap.extractPluginId(attrs));
    }

    @Test
    void extractPluginId_multipleEntryPoints() {
        Attributes attrs = new Attributes();
        attrs.putValue("Entry-Points", "com.example.Other com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_jdbc");
        assertEquals(Collections.singletonList("jdbc"), Bootstrap.extractPluginId(attrs));
    }

    @Test
    void extractPluginId_multipleEntryPoints2() {
        Attributes attrs = new Attributes();
        attrs.putValue("Entry-Points", "com.example.Other com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_jdbc com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_spring");
        assertEquals(Arrays.asList("jdbc", "spring"), Bootstrap.extractPluginId(attrs));
    }

    @Test
    void extractPluginId_noPluginId() {
        Attributes attrs = new Attributes();
        attrs.putValue("Entry-Points", "com.example.SomeOtherClass");
        assertEquals(Collections.emptyList(), Bootstrap.extractPluginId(attrs));
    }

    @Test
    void extractPluginId_nullAttributes() {
        assertEquals(Collections.emptyList(), Bootstrap.extractPluginId(null));
    }

    @Test
    void extractPluginId_emptyAttributes() {
        Attributes attrs = new Attributes();
        assertEquals(Collections.emptyList(), Bootstrap.extractPluginId(attrs));
    }
}
