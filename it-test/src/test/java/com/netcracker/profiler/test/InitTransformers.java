package com.netcracker.profiler.test;

import static org.junit.jupiter.api.Assertions.fail;

import com.netcracker.profiler.agent.ProfilingTransformer;
import com.netcracker.profiler.agent.plugins.EnhancerRegistryPluginImpl;
import com.netcracker.profiler.configuration.ConfigurationImpl;
import com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_test;

import mockit.internal.startup.Startup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.UnmodifiableClassException;
import java.lang.reflect.InvocationTargetException;
import java.security.ProtectionDomain;

import javax.xml.parsers.ParserConfigurationException;

/**
 * Prepares given class for testing (loads it through profiling transformer)
 */
public abstract class InitTransformers {
    public static final Logger log = LoggerFactory.getLogger(InitTransformers.class);

    static {
        try {
            main(null);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static boolean transformerAdded;

    private static void main(String[] args) throws IOException, SAXException, ParserConfigurationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchFieldException, UnmodifiableClassException {
        addTransformer();
    }

    public static synchronized void addTransformer() throws IOException, SAXException, ParserConfigurationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchFieldException, UnmodifiableClassException {
        if (transformerAdded)
            return;
        transformerAdded = true;

        String fileName = "src/test/resources/config/test_config.xml";
        if (!new File(fileName).exists())
            fileName = "it-test/" + fileName;

        if (!new File(fileName).exists())
            fail("Configuration file " + fileName + " is not found");

        new EnhancerRegistryPluginImpl();
        new EnhancerPlugin_test();

        ConfigurationImpl conf = new ConfigurationImpl(fileName);
        final ProfilingTransformer pt = new ProfilingTransformer(conf);
        log.info("Using configuration {} for transformer {}", fileName, pt);

        Startup.instrumentation().
                addTransformer(
                        new ClassFileTransformer() {
                            public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
                                if (!className.startsWith("com/netcracker/profiler/test"))
                                    return null;
                                log.info("Transforming class {}", className);
                                try {
                                    return pt.transform(loader, className, classBeingRedefined, protectionDomain, classfileBuffer);
                                } catch (Throwable e) {
                                    log.error("Unable to transform class " + className, e);
                                }
                                return null;
                            }
                        });
    }
}
