package org.qubership.profiler.agent;

import org.qubership.profiler.agent.plugins.ConfigurationSPI;
import org.qubership.profiler.configuration.Rule;
import org.qubership.profiler.instrument.TypeUtils;
import org.qubership.profiler.util.IOHelper;

import java.io.*;
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ConfigurationReloader implements Runnable {
//    public static final Logger log = LoggerFactory.getLogger(ConfigurationReloader.class);
    private static final ESCLogger logger = ESCLogger.getLogger(ConfigurationReloader.class.getName());
    private static boolean NEED_REFLECTION_WA =
            !(System.getProperty("java.vendor").startsWith("Oracle")
                    || System.getProperty("java.vendor").startsWith("Sun")
            );

    private final ConfigurationSPI conf;
    private final ConfigurationSPI newConf;
    private final Set<String> classNames;
    private final Instrumentation inst;
    private final ReloadStatusMutable reloadStatus;
    private final Semaphore reloadingSemaphore;
    private final ArrayList<String> firstReloadedClasses = new ArrayList<String>();

    public ConfigurationReloader(ConfigurationSPI conf, ConfigurationSPI newConf, Set<String> classNames, Instrumentation inst, ReloadStatusMutable reloadStatus, Semaphore reloadingSemaphore) {
        this.conf = conf;
        this.newConf = newConf;
        this.classNames = classNames;
        this.inst = inst;
        this.reloadStatus = reloadStatus;
        this.reloadingSemaphore = reloadingSemaphore;
    }

    public void run() {
        final ReloadStatusMutable reloadStatus = this.reloadStatus;
        final Instrumentation inst = this.inst;
        try {
            reloadStatus.setMessage("Calculating classes to be reloaded");
            ArrayList<Rule> rules = new ArrayList<Rule>();
            ArrayList<Rule> newRules = new ArrayList<Rule>();
            final Class[] allClasses = inst.getAllLoadedClasses();
            Map<String, Collection<Class>> jarToClasses = new HashMap<String, Collection<Class>>();

            final ArrayList<Class> classesWithUnknownSource = new ArrayList<Class>();
            jarToClasses.put(null, classesWithUnknownSource);
            reloadStatus.setTotalCount(allClasses.length);
            int classesToReload = 0;
            final Set<String> classNames = this.classNames;
            final ConfigurationSPI newConf = this.newConf;
            final ConfigurationSPI conf = this.conf;
            for (int i1 = 0, allClassesLength = allClasses.length; i1 < allClassesLength; i1++) {
                reloadStatus.setSuccessCount(i1);
                Class clazz = allClasses[i1];
                String className = clazz.getName();
                if (className == null) continue;
                if (classNames != null && !classNames.contains(className)) continue;
                if (newConf != null) {
                    if (conf == null && clazz.getClassLoader() != null) continue;
                    String nativeClassName = className.replace('.', '/');
                    newConf.getRulesForClass(nativeClassName, newRules);
                    if (conf != null) {
                        conf.getRulesForClass(nativeClassName, rules);
                        if (rules.equals(newRules)) continue;
                    } else if (newRules.isEmpty()) continue;
                }
                classesToReload++;
                final String fullJarName = TypeUtils.getFullJarName(clazz.getProtectionDomain());
                Collection<Class> classes = jarToClasses.get(fullJarName);
                if (classes == null)
                    jarToClasses.put(fullJarName, classes = new ArrayList<Class>());
                classes.add(clazz);
            }

            performReload(jarToClasses, classesWithUnknownSource, classesToReload);
        } catch (Throwable t) {
            reloadStatus.setMessage("Error while reloading classes: " + t.getMessage() + ". Please, refer to the profiler.log for the details.");
            logger.log(Level.WARNING, "Error while reloading classes", t);
        } finally {
            reloadStatus.setDone(true);
            reloadingSemaphore.release();
        }
    }

    private void performReload(Map<String, Collection<Class>> jarToClasses, ArrayList<Class> classesWithUnknownSource, int classesToReload) {
        logger.info("About to reload "+classesToReload+" classes in "+jarToClasses.size()+" different locations");
        if (NEED_REFLECTION_WA) {
            logger.info("Will call clazz.getMethods() for each class before reload to workaround issue https://github.com/eclipse/openj9/issues/1950");
        }

        final ReloadStatusMutable reloadStatus = this.reloadStatus;
        reloadStatus.setMessage("Reloading");
        reloadStatus.setSuccessCount(0);
        reloadStatus.setErrorCount(0);
        reloadStatus.setTotalCount(classesToReload);
        for (Map.Entry<String, Collection<Class>> entry : jarToClasses.entrySet()) {
            final String jarLocation = entry.getKey();
            if (jarLocation != null) {
                reloadStatus.setMessage("Processing " + jarLocation);
                /* try open jar file manually */
                final File location = new File(jarLocation);

                if (!location.exists()) {
                    classesWithUnknownSource.addAll(entry.getValue());
                    continue;
                }

                logger.info("About to process "+entry.getValue().size()+" classes from "+location.getAbsolutePath());
                if (location.isFile()) {
                    reloadClassesFromJar(classesWithUnknownSource, entry, location);
                    continue;
                }

                if (location.isDirectory()) {
                    reloadClassesFromDirectory(classesWithUnknownSource, entry, location);
                    continue;
                }

                classesWithUnknownSource.addAll(entry.getValue());
            }
            final int successCount = reloadStatus.getSuccessCount();
            if (successCount % 50 == 0 && successCount > 0)
                logger.info("Processed "+successCount + reloadStatus.getErrorCount()+" of "+classesToReload+" classes");
        }

        logger.fine("Processing " + classesWithUnknownSource.size() + " classes with unknown location"  );

        reloadStatus.setMessage("Processing classes with unknown class file location");
        for (int i = 0, classesWithUnknownSourceSize = classesWithUnknownSource.size(); i < classesWithUnknownSourceSize; i++) {
            Class clazz = classesWithUnknownSource.get(i);
            final String originalClassName = clazz.getName();
            String nativeClassName = originalClassName.replace('.', '/');
            URL resource = clazz.getResource("/" + nativeClassName + ".class");
            if (resource == null) {
                logger.warning("Unable to find class file for " + originalClassName);
                continue;
            }
            final InputStream is;
            try {
                is = resource.openStream();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Unable to find class " + originalClassName + " in classpath", e);
                continue;
            }

            if (is != null)
                reloadClassFromStream(clazz, is, resource.toString());
            else {
                logger.warning("Unable to find class file for class "+ clazz.getName() +" using getResourceAsStream");
                reloadStatus.setErrorCount(reloadStatus.getErrorCount() + 1);
            }
            if (i % 50 == 0 && i > 0) {
                logger.info("Processed "+i+" of "+classesWithUnknownSourceSize+" classes");
                reloadStatus.setMessage("Processed " + originalClassName);
            }
        }
        StringBuilder msg = new StringBuilder();
        msg.append("Reload complete. Reloaded ");
        msg.append(reloadStatus.getSuccessCount()).append(" class");
        if (reloadStatus.getSuccessCount() != 1) msg.append("es");
        if (reloadStatus.getErrorCount() > 0)
            msg.append(" (").append(reloadStatus.getErrorCount()).append(" more failed to reload). ");
        if (!firstReloadedClasses.isEmpty()) {
            msg.append(firstReloadedClasses.get(0));
            for (int i = 1, firstReloadedClassesSize = firstReloadedClasses.size(); i < firstReloadedClassesSize; i++) {
                msg.append(", ").append(firstReloadedClasses.get(i));
            }
        }
        reloadStatus.setMessage(msg.toString());
    }

    private void reloadClassesFromDirectory(ArrayList<Class> classesWithUnknownSource, Map.Entry<String, Collection<Class>> entry, File location) {
        final String locationAbsolutePath = location.getAbsolutePath();
        for (Class clazz : entry.getValue()) {
            String nativeClassName = clazz.getName().replace('.', '/');
            FileInputStream is = null;
            try {
                is = new FileInputStream(new File(location, nativeClassName + ".class"));
            } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, "Unable to find class "+ clazz.getName() +" in folder " + entry.getKey(), e);
            }
            if (is == null || !reloadClassFromStream(clazz, is, locationAbsolutePath))
                classesWithUnknownSource.add(clazz);
        }
    }

    private void reloadClassesFromJar(ArrayList<Class> classesWithUnknownSource, Map.Entry<String, Collection<Class>> entry, File location) {
        final String locationAbsolutePath = location.getAbsolutePath();
        if (locationAbsolutePath.endsWith(".class")) {
            InputStream is;
            for (Class clazz : entry.getValue()) {
                try {
                    is = new FileInputStream(location);
                    if (!reloadClassFromStream(clazz, is, locationAbsolutePath))
                        logger.warning("Unable to reload class " + clazz.getName() + " from file {} " +  locationAbsolutePath);
                } catch (FileNotFoundException e) {
                    logger.log(Level.WARNING, "Unable to open input stream", e);
                }
            }
            return;
        }

        ZipFile zip;
        try {
            zip = new ZipFile(location);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Unable to reload classes from  " + locationAbsolutePath, e);
            classesWithUnknownSource.addAll(entry.getValue());
            return;
        }

        try {
            for (Class clazz : entry.getValue()) {
                String nativeClassName = clazz.getName().replace('.', '/');
                final ZipEntry ze = zip.getEntry(nativeClassName + ".class");
                InputStream is = null;
                if (ze != null)
                    try {
                        is = zip.getInputStream(ze);
                    } catch (IOException e) {
                        logger.log(Level.WARNING, "Unable to open entry " + ze.getName() + " in file " + entry.getKey(), e);
                    }
                if (is == null || !reloadClassFromStream(clazz, is, locationAbsolutePath))
                    classesWithUnknownSource.add(clazz);
            }
        } finally {
            try {
                zip.close();
            } catch (IOException e) {
                 /**/
            }
        }
    }

    private boolean reloadClassFromStream(Class clazz, InputStream is, String source) {
        final ReloadStatusMutable reloadStatus = this.reloadStatus;
        try {
            logger.info("Reloading class "+clazz.getName()+" from "+source);
            if (NEED_REFLECTION_WA) {
                try {
                  // Workaround for J9, PSUPAI-4834
                  clazz.getMethods();
                } catch (Throwable t) {
                    logger.log(Level.WARNING, "Problem during preloading methods via reflection for class " +  clazz.getName(), t);
                }
            }
            final byte[] bytes = IOHelper.readFully(is);
            inst.redefineClasses(new ClassDefinition(clazz, bytes));
            logger.fine("Successfully reloaded " + clazz.getName());
            if (firstReloadedClasses.size() < 20)
                firstReloadedClasses.add(clazz.getName());
            reloadStatus.setSuccessCount(reloadStatus.getSuccessCount() + 1);
            return true;
        } catch (Throwable t) {
            logger.log(Level.WARNING, "Unable to reload class  " +  clazz.getName(), t);
            reloadStatus.setErrorCount(reloadStatus.getErrorCount() + 1);
            if (firstReloadedClasses.size() < 20)
                firstReloadedClasses.add(clazz.getName() + " - fail");
        } finally {
            IOHelper.close(is);
        }
        return false;
    }
}
