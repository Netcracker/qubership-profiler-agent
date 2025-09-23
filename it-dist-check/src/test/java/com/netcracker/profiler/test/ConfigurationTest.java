package com.netcracker.profiler.test;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.Configuration;
import mockit.internal.startup.Startup;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;

import java.io.File;
import java.io.IOException;
import java.util.jar.JarFile;

public class ConfigurationTest extends BaseConfigurationTest {
    @BeforeSuite
    public void defaultConfigurationLoads() throws IOException {
        println("Loading default configuration");
        String path = "target/dependency/applications/execution-statistics-collector";
        String jarPath = "target/dependency/applications/execution-statistics-collector/lib/boot.jar";
        if (!new File(path).exists()) {
            path = "it-dist-check/" + path;
            jarPath = "it-dist-check/" + jarPath;
        }
        Startup.instrumentation().appendToBootstrapClassLoaderSearch(new JarFile(jarPath));
        System.setProperty("profiler.home", path);
        Bootstrap.premain(null, Startup.instrumentation());
        Configuration conf = Bootstrap.getPlugin(Configuration.class);
        Assert.assertNotNull(conf, "Bootstrap.getPlugin(Configuration.class) returned null");
    }
}
