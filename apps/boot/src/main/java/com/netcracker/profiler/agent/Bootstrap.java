package com.netcracker.profiler.agent;

import java.io.*;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.logging.Level;

public class Bootstrap {
    public static final List<String> BOOT_PACKAGES = Arrays.asList("com.netcracker.profiler.agent", "com.netcracker.profiler.agent.http");
    private static Instrumentation inst;
    private static final Map<Class, Object> plugins = new HashMap<Class, Object>();
    private static final ESCLogger logger = ESCLogger.getLogger(Bootstrap.class, (DumpRootResolverAgent.VERBOSE ? Level.FINE : ESCLogger.ESC_LOG_LEVEL));

    static class PluginJarInfo {
        final String jarPath;
        final List<String> pluginId;
        final String version;

        PluginJarInfo(String jarPath, List<String> pluginId, String version) {
            this.jarPath = jarPath;
            this.pluginId = pluginId;
            this.version = version;
        }
    }

    private static int JAVA_VERSION;
    static  {
        String version = System.getProperty("java.version");
        if(version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if(dot != -1) { version = version.substring(0, dot); }
        }
        try {
            JAVA_VERSION = Integer.parseInt(version);
        } catch (NumberFormatException e){
            logger.severe("Failed to parse java version from string " + version, e);
            JAVA_VERSION = -1;
        }
        logger.fine("Java version is determined to be " + JAVA_VERSION);
    }

    public static void info(String x) {
        if (DumpRootResolverAgent.VERBOSE) {
            logger.info(x);
        }
    }

    public static void premain(String agentArgs, Instrumentation inst) {
        if (Bootstrap.inst != null) {
            logger.fine("Profiler: it looks like you have specified javaagent:profiler-agent.jar option twice. Second one will not work");

            return;
        }
        addJBossModulesSystemPkg();
        Bootstrap.inst = inst;
        List<String> plugins = split(agentArgs);
        if (plugins.isEmpty()) {
            File lib = new File(DumpRootResolverAgent.PROFILER_HOME, "lib");
            File[] jars = lib.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".jar");
                }
            });

            if (jars != null) {
                plugins = new ArrayList<String>();
                for (File jar : jars) {
                    plugins.add(jar.getAbsolutePath());
                }
            }
        }

        if (plugins.isEmpty()) {

            throw new IllegalArgumentException("Profiler: bootstrap argument was not specified and was not autodetected. " +
                    "To specify jars explicitly, please use comma separated list as follows: -javaagent:full/path/to/profiler.jar=lib/a.jar,lib/b.jar");
        }

        loadPlugins(plugins);

        ProfilerTransformerPlugin tr = getPlugin(ProfilerTransformerPlugin.class);
        if (tr == null)
            logger.fine("Profiler: no profiling transformer loaded. Total number of loaded plugins is " + plugins.size());
        else
            logger.info("Profiler: initialized, version " + getImplementationVersion(tr.getClass()));
    }

    private static void addJBossModulesSystemPkg() {
        String pkgs = System.getProperty("jboss.modules.system.pkgs");
        String profilerPackage = Bootstrap.class.getPackage().getName();
        if (pkgs == null) {
            pkgs = profilerPackage;
        } else {
            pkgs += "," + profilerPackage;
            // Replace invalid package if specified
            pkgs = pkgs.replace("com.netcracker.profiler,", "");
        }
        System.setProperty("jboss.modules.system.pkgs", pkgs);
    }

    private static List<String> split(String args) {
        if (args == null) return Collections.emptyList();
        List<String> res = new ArrayList<String>();
        for (StringTokenizer stringTokenizer = new StringTokenizer(args, ","); stringTokenizer.hasMoreTokens(); ) {
            res.add(stringTokenizer.nextToken());
        }
        return res;
    }

    private static boolean pluginSupported(String jarName){
        if(jarName.endsWith("reactor-instrument.jar") && JAVA_VERSION < 8){
            logger.fine("plugin " + jarName + " is not supported");
            return false;
        }
        return true;
    }

    /**
     * Extract Plugin-Id from JAR manifest attributes.
     * Falls back to extracting from Entry-Points if Plugin-Id is not present.
     */
    static List<String> extractPluginId(Attributes attrs) {
        if (attrs == null) {
            return Collections.emptyList();
        }
        String pluginId = attrs.getValue("Plugin-Id");
        if (pluginId != null) {
            return Collections.singletonList(pluginId);
        }
        // Fallback: extract from Entry-Points (EnhancerPlugin_XXX -> XXX)
        // Example: com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_activemq
        String entryPoints = attrs.getValue("Entry-Points");
        List<String> result = new ArrayList<>();
        if (entryPoints != null) {
            for (String entry : entryPoints.split("\\s+")) {
                int idx = entry.lastIndexOf("EnhancerPlugin_");
                if (idx >= 0) {
                    String suffix = entry.substring(idx + "EnhancerPlugin_".length());
                    if (!suffix.isEmpty()) {
                        result.add(suffix);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Read Plugin-Id and Implementation-Version from a JAR file.
     */
    private static PluginJarInfo readPluginJarInfo(String jarPath) {
        try (JarInputStream jis = new JarInputStream(Files.newInputStream(Paths.get(jarPath)))) {
            Manifest man = jis.getManifest();
            if (man == null) {
                return null;
            }
            Attributes attrs = man.getMainAttributes();
            List<String> pluginId = extractPluginId(attrs);
            if (pluginId.isEmpty()) {
                return null;
            }
            String version = attrs.getValue("Implementation-Version");
            return new PluginJarInfo(jarPath, pluginId, version);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Profiler: unable to read manifest from " + jarPath, e);
            return null;
        }
    }

    /**
     * Check for duplicate Plugin-Ids and exclude all duplicates from loading.
     * JARs with unique Plugin-Id are loaded normally.
     * JARs with duplicate Plugin-Id are skipped and a warning is logged.
     */
    private static List<String> deduplicatePlugins(List<String> plugins) {
        // Collect info for JAR files with Plugin-Id
        Map<String, List<PluginJarInfo>> byPluginId = new LinkedHashMap<String, List<PluginJarInfo>>();

        List<String> result = new ArrayList<String>();

        for (String jarPath : plugins) {
            PluginJarInfo info = readPluginJarInfo(jarPath);
            if (info != null) {
                for (String pluginId : info.pluginId) {
                    byPluginId.computeIfAbsent(pluginId, k -> new ArrayList<>())
                            .add(info);
                }
            } else if (!jarPath.endsWith(".jar")) {
                // E.g. .class explicitly passed from the command line
                result.add(jarPath);
            }
        }

        Set<String> problematicJars = new HashSet<>();
        // Detect problematic jars: they have plugins with overlapping ids
        String lib = new File(DumpRootResolverAgent.PROFILER_HOME).getAbsolutePath();
        for (Map.Entry<String, List<PluginJarInfo>> entry : byPluginId.entrySet()) {
            List<PluginJarInfo> jars = entry.getValue();
            if (jars.size() == 1) {
                // If the plugin is included in a single jar only, add the jar to the resulting list
                // It might be removed later if there's an overlap with another plugin id
                if (!problematicJars.contains(jars.get(0).jarPath)) {
                    result.add(jars.get(0).jarPath);
                }
            } else {
                // Duplicate Plugin-Id found, skip ALL of them and log warning
                StringBuilder sb = new StringBuilder();
                sb.append("Profiler: Duplicate Plugin-Id '").append(entry.getKey()).append("' found in multiple JARs. ");
                sb.append("None of them will be loaded. Please remove the duplicate JAR(s):\n");
                for (PluginJarInfo jar : jars) {
                    if (problematicJars.add(jar.jarPath)) {
                        // If the jar was n
                        result.remove(jar.jarPath);
                    }
                    sb.append("  - ").append(jar.jarPath.replace(lib, "$esc"));
                    if (jar.version != null) {
                        sb.append(" (version=").append(jar.version).append(")");
                    }
                    sb.append("\n");
                }
                logger.warning(sb.toString());
            }
        }
        return result;
    }

    private static void loadPlugins(List<String> plugins) {
        List<String> deduplicated = deduplicatePlugins(plugins);
        List<String> ordered = sortPlugins(deduplicated);
        List<Object> impls = new ArrayList<Object>();
        String lib = new File(DumpRootResolverAgent.PROFILER_HOME).getAbsolutePath();

        for (String jarName : ordered) {
            try {
                if(!pluginSupported(jarName)){
                    continue;
                }
                if (jarName.endsWith(".class")) {
                    callMain(jarName.substring(0, jarName.length() - 6));
                } else if (jarName.endsWith(".jar")) {
                    if (jarName.endsWith("reactor-instrument.jar")) {
                        Instrumentation instrumentation = getInstrumentation();
                        instrumentation.appendToSystemClassLoaderSearch(new JarFile(jarName));
                    }
                    final PluginClassLoader loader = PluginClassLoader.newInstance(jarName);
                    if (loader != null) {
                        info("Profiler: loading " + jarName.replace(lib, "$esc"));
                        impls.addAll(loader.startPlugin());
                    } else if (!jarName.endsWith("agent.jar") && !jarName.endsWith("boot.jar")) {
                        info("Profiler: jar " + jarName + " was skipped since it does not contain entry points");
                    }
                } else
                    logger.warning("Profiler: unknown argument " + jarName + ". Expecting *.class or *.jar");
            } catch (Throwable e) {
                throw new RuntimeException("Unable to load plugin " + jarName, e);
            }
        }
        for (Object impl : impls) {
            if (impl instanceof TwoPhaseInit) {
                try {
                    ((TwoPhaseInit) impl).start();
                } catch (Throwable e) {
                    throw new RuntimeException("Unable to start plugin " + impl, e);
                }
            }
        }
    }

    private static List<String> sortPlugins(List<String> plugins) {
        if (plugins.size() < 2) return plugins;
        List<String> res = new ArrayList<String>(plugins);
        Collections.sort(res, new Comparator<String>() {
            public int compare(String o1, String o2) {
                return o1.endsWith("runtime.jar") ? -1 : o2.endsWith("runtime.jar") ? 1 : o1.compareTo(o2);
            }
        });
        return res;
    }

    private static void callMain(String className) {
        try {
            logger.fine("Profiler: about to invoke main method on class " + className);
            ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
            if (systemClassLoader == null) {
                logger.warning("Profiler: system classloader not found. Execution of " + className + " is skipped");

                return;
            }
            Class<?> aClass = systemClassLoader.loadClass(className);
            Method main = aClass.getMethod("main", String[].class);
            main.invoke(null, new Object[]{null});
        } catch (ClassNotFoundException e) {
            logger.severe("Profiler: Unable to load class " + className + " as it is not found");
        } catch (NoSuchMethodException e) {
            logger.log(Level.SEVERE, "Profiler: Unable to find main(String[]) method in class " + className, e);
        } catch (InvocationTargetException | IllegalAccessException e) {
            logger.log(Level.SEVERE, "Profiler: Unable to invoke main(String[]) method in class " + className, e);
        }
    }

    public static Instrumentation getInstrumentation() {
        return inst;
    }

    public static<T> void registerPlugin(Class<T> type, T value){
        plugins.put(type, value);
    }

    public static<T> T getPlugin(Class<T> type){
        return (T) plugins.get(type);
    }

    public static<T> T getPluginOrNull(Class<?> type, Class<T> interfaceType){
        Object intended = getPlugin(type);
        if( intended == null || !interfaceType.isAssignableFrom(intended.getClass())) {
            return null;
        }
        return (T) intended;
    }

    public static String getImplementationVersion(Class klass) {
        ProtectionDomain pd = klass.getProtectionDomain();
        if (pd == null) return "unknown (no protection domain)";
        CodeSource cs = pd.getCodeSource();
        if (cs == null) return "unknown (no code source)";
        URL loc = cs.getLocation();
        if (loc == null) return "unknown (no location)";
        JarInputStream is = null;
        try {
            is = new JarInputStream(loc.openStream());
            Manifest man = is.getManifest();
            if (man == null) return "unknown (no manifest)";
            Attributes attr = man.getMainAttributes();
            return attr.getValue("Implementation-Version") + ", build date " + attr.getValue("Build-Time");
        } catch (IOException e) {
            logger.log(Level.WARNING, "", e);
            return "unknown (unable to read manifest)";
        } finally {
            if (is != null) try {
                is.close();
            } catch (IOException e) { /**/ }
        }
    }
}
