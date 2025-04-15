package org.qubership.profiler.agent.plugins;

import org.qubership.profiler.agent.*;
import org.qubership.profiler.configuration.ConfigurationImpl;
import org.qubership.profiler.configuration.PropertyFacade;
import org.qubership.profiler.dump.DumpRootResolver;

import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import javax.xml.parsers.ParserConfigurationException;

public class ProfilerTransformerPluginImpl implements org.qubership.profiler.agent.ProfilerTransformerPlugin_01, TwoPhaseInit {
    private static final ESCLogger logger = ESCLogger.getLogger(ProfilerTransformerPluginImpl.class.getName());
    final ProfilingTransformer profiler;
    final ReloadStatusMutable reloadStatus = new ReloadStatusImpl();
    private final Semaphore reloadingSemaphore = new Semaphore(1);

    public ProfilerTransformerPluginImpl() {
        configureLogback();

        profiler = new ProfilingTransformer(null);
        Bootstrap.registerPlugin(ProfilerTransformerPlugin.class, this);
        new LocalState(); // Ensure no dynamic loading will occur while using LocalState
    }

    public void start() throws IOException, SAXException, ParserConfigurationException {
        String configPath = DumpRootResolver.CONFIG_FILE;
        ConfigurationSPI conf = new ConfigurationImpl(configPath);
        profiler.setConfiguration(conf);
        ProfilerData.properties = conf.getProperties();
        reloadStatus.setConfigPath(conf.getConfigFile());
        reloadStatus.setMessage("Initial configuration");
        Bootstrap.getInstrumentation().addTransformer(profiler, true);
    }

    private void configureLogback() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        String logbackXmlPath = PropertyFacade.getProperty("profiler.log.config", null);

        if (logbackXmlPath == null) {
            File file = new File(new File(DumpRootResolver.PROFILER_HOME), "config/logback.xml");
            if (file.exists()) {
                logbackXmlPath = file.getAbsolutePath();
            }
        }

        if (logbackXmlPath == null)
            logger.warning("Profiler: unable to find logback.xml configuration file, please place it near _config.xml or specify its location via profiler.log.config property");


        try {
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            // the context was probably already configured by default configuration rules
            lc.reset();
            if (logbackXmlPath != null) {
                logger.fine("Profiler: reading logback configuration from " + logbackXmlPath);

                configurator.doConfigure(logbackXmlPath);
            } else {
                logger.fine("Profiler: trying to find logback.xml in classpath");
                configurator.doConfigure(getClass().getResourceAsStream("logback.xml"));
            }
        } catch (JoranException je) {
            logger.log(Level.SEVERE, "Unable to load logging configuration from " + logbackXmlPath, je);
            new BasicConfigurator().configure(lc);
        }
        ProfilerData.pluginLogger = new ProfilerPluginLoggerImpl();
    }

    public Configuration getConfiguration() {
        return profiler.getConfiguration();
    }

    public ReloadStatus getReloadStatus() {
        return reloadStatus;
    }

    public void reloadConfiguration(String newConfigPath) throws IOException, SAXException, ParserConfigurationException {
        if (!reloadingSemaphore.tryAcquire())
            throw new IllegalStateException("Reload is in progress. Please, try later");
        boolean threadStarted = false;
        try {
            reloadStatus.setMessage("Initializing");
            reloadStatus.setDone(false);
            reloadStatus.setTotalCount(1);
            reloadStatus.setSuccessCount(0);
            reloadStatus.setErrorCount(0);
            ConfigurationSPI conf = profiler.getConfiguration();
            reloadStatus.setConfigPath(conf.getConfigFile());
            if (newConfigPath == null || newConfigPath.length() == 0) {
                logger.warning("New configuration path was not set, reusing the path of active configuration");
                newConfigPath = conf.getConfigFile();
            }
            logger.info("Reloading configuration from " + newConfigPath);
            reloadStatus.setMessage("Loading configuration from " + newConfigPath);
            ConfigurationSPI newConf = new ConfigurationImpl(newConfigPath);
            if (newConf.equals(conf)) {
                reloadStatus.setMessage("New configuration is identical to the previously loaded one");
                reloadStatus.setDone(true);
                reloadStatus.setSuccessCount(1);
                return;
            }

            reloadStatus.setConfigPath(newConf.getConfigFile());
            ProfilerData.properties = conf.getProperties();
            profiler.setConfiguration(newConf);

            final org.qubership.profiler.agent.DumperPlugin dumper = Bootstrap.getPlugin(DumperPlugin.class);
            try {
                if (dumper instanceof org.qubership.profiler.agent.DumperPlugin_01) {
                    org.qubership.profiler.agent.DumperPlugin_01 dumper01 = (DumperPlugin_01) dumper;
                    dumper01.reconfigure();
                }
            }
            catch (Throwable t) {
                logger.log(Level.WARNING, "Unable to reconfigure dumper", t);
            }

            final Instrumentation inst = Bootstrap.getInstrumentation();
            if (!inst.isRedefineClassesSupported()) {
                reloadStatus.setMessage("JVM does not support class redefinition. Only newly loaded classes would use new configuration");
                reloadStatus.setDone(true);
                reloadStatus.setErrorCount(1);
                return;
            }

            new Thread(new ConfigurationReloader(conf, newConf, null, inst, reloadStatus, reloadingSemaphore)).start();
            threadStarted = true;
        } finally {
            if (!threadStarted) {
                reloadStatus.setDone(true);
                reloadingSemaphore.release();
            }
        }
    }

    public void reloadClasses(Set<String> classNames) throws IOException, SAXException, ParserConfigurationException {
        if (!reloadingSemaphore.tryAcquire())
            throw new IllegalStateException("Reload is in progress. Please, try later");
        boolean threadStarted = false;
        try {
            reloadStatus.setMessage("Initializing");
            reloadStatus.setDone(false);
            reloadStatus.setTotalCount(1);
            reloadStatus.setSuccessCount(0);
            reloadStatus.setErrorCount(0);

            final Instrumentation inst = Bootstrap.getInstrumentation();
            if (!inst.isRedefineClassesSupported()) {
                reloadStatus.setMessage("JVM does not support class redefinition. Only newly loaded classes would use new configuration");
                reloadStatus.setDone(true);
                reloadStatus.setErrorCount(1);
                return;
            }

            new ConfigurationReloader(null, profiler.getConfiguration(), classNames, inst, reloadStatus, reloadingSemaphore).run();
            threadStarted = true;
        } finally {
            if (!threadStarted) {
                reloadStatus.setDone(true);
                reloadingSemaphore.release();
            }
        }
    }
}
