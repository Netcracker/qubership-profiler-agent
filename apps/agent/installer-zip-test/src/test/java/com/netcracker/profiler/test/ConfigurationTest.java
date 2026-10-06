package com.netcracker.profiler.test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.Configuration;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ConfigurationTest {
    @Test
    public void defaultConfigurationLoads() throws IOException {
        Configuration conf = Bootstrap.getPlugin(Configuration.class);
        assertNotNull(conf, "Bootstrap.getPlugin(Configuration.class) should always be present");
    }
}
