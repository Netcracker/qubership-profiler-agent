package com.netcracker.profiler.agent;

import java.io.*;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
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
        final Set<String> pluginIds;
        final String version;

        PluginJarInfo(String jarPath, Set<String> pluginIds, String version) {
            this.jarPath = jarPath;
            this.pluginIds = pluginIds;
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
        try {
            startProfiler(agentArgs);
        } catch (Throwable e) {
            // libinstrument aborts the JVM on any exception leaving premain, killing the
            // application before it runs a line of its own. Startup failures stop here instead,
            // and the application keeps running unprofiled.
            logger.severe("Profiler: initialization failed, the application continues without profiling", e);
        }
    }

    private static void startProfiler(String agentArgs) {
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
     * Collects every identity a JAR manifest claims for its plugin, so that two JARs shipping the
     * same plugin can be recognized as duplicates.
     *
     * <p>A JAR is identified by its {@code Plugin-Id}, by the enhancer names in its
     * {@code Entry-Points} ({@code ...EnhancerPlugin_activemq} yields {@code activemq}), and by the
     * entry-point set as a whole. The entry-point set is what matches a JAR built before
     * {@code Plugin-Id} existed with the release that introduced the attribute.
     *
     * @return every identity the manifest declares, empty when it declares no entry points
     */
    static Set<String> extractPluginIds(Attributes attrs) {
        if (attrs == null) {
            return Collections.emptySet();
        }
        String entryPoints = attrs.getValue("Entry-Points");
        if (entryPoints == null || entryPoints.trim().isEmpty()) {
            // Not a plugin JAR: agent.jar and boot.jar take this path.
            return Collections.emptySet();
        }
        Set<String> result = new LinkedHashSet<>();
        String pluginId = attrs.getValue("Plugin-Id");
        if (pluginId != null) {
            result.add(pluginId);
        }
        // Example: com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_activemq
        String[] entries = entryPoints.trim().split("\\s+");
        for (String entry : entries) {
            int idx = entry.lastIndexOf("EnhancerPlugin_");
            if (idx >= 0) {
                String suffix = entry.substring(idx + "EnhancerPlugin_".length());
                if (!suffix.isEmpty()) {
                    result.add(suffix);
                }
            }
        }
        String[] sortedEntries = entries.clone();
        Arrays.sort(sortedEntries);
        result.add("entry-points:" + String.join(" ", sortedEntries));
        return result;
    }

    /**
     * Reads the plugin identities and {@code Implementation-Version} from a JAR file.
     *
     * @return {@code null} when the file carries no plugin, or cannot be read
     */
    private static PluginJarInfo readPluginJarInfo(String jarPath) {
        try (JarInputStream jis = new JarInputStream(Files.newInputStream(Paths.get(jarPath)))) {
            Manifest man = jis.getManifest();
            if (man == null) {
                return null;
            }
            Attributes attrs = man.getMainAttributes();
            Set<String> pluginIds = extractPluginIds(attrs);
            if (pluginIds.isEmpty()) {
                return null;
            }
            String version = attrs.getValue("Implementation-Version");
            return new PluginJarInfo(jarPath, pluginIds, version);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Profiler: unable to read manifest from " + jarPath, e);
            return null;
        }
    }

    /**
     * Compares {@code Implementation-Version} values so the newest copy of a duplicated plugin can
     * be picked. Numeric segments compare numerically, a qualifier loses to the plain release it
     * qualifies ({@code 4.0.5-SNAPSHOT} &lt; {@code 4.0.5}), and a missing version loses to any
     * version.
     */
    static int compareVersions(String left, String right) {
        if (left == null || right == null) {
            return left == null ? (right == null ? 0 : -1) : 1;
        }
        String[] leftParts = left.split("[.\\-+_]");
        String[] rightParts = right.split("[.\\-+_]");
        for (int i = 0; i < Math.max(leftParts.length, rightParts.length); i++) {
            String l = i < leftParts.length ? leftParts[i] : null;
            String r = i < rightParts.length ? rightParts[i] : null;
            if (l == null) {
                // 4.0 < 4.0.1, but 4.0.5 > 4.0.5-SNAPSHOT
                return isNumeric(r) ? -1 : 1;
            }
            if (r == null) {
                return isNumeric(l) ? 1 : -1;
            }
            int cmp = isNumeric(l) && isNumeric(r)
                    ? new BigInteger(l).compareTo(new BigInteger(r))
                    : l.compareToIgnoreCase(r);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * Keeps a single JAR per plugin, so that a stale copy left next to the current one cannot make
     * the profiler load one plugin twice.
     *
     * <p>Loading a plugin twice gives each copy its own {@link PluginClassLoader}, and classes from
     * one copy then fail to cast to the same-named classes of the other. The newest copy wins, and
     * the JARs it displaces are named in a warning so the stale files can be removed. Files that
     * carry no plugin at all pass through untouched.
     */
    private static List<String> deduplicatePlugins(List<String> plugins) {
        Map<String, List<PluginJarInfo>> byPluginId = new LinkedHashMap<String, List<PluginJarInfo>>();
        List<String> result = new ArrayList<String>();

        for (String jarPath : plugins) {
            PluginJarInfo info = readPluginJarInfo(jarPath);
            if (info == null) {
                // Carries no plugin: a .class explicitly passed on the command line, agent.jar,
                // boot.jar, or a JAR whose manifest could not be read.
                result.add(jarPath);
                continue;
            }
            for (String pluginId : info.pluginIds) {
                byPluginId.computeIfAbsent(pluginId, k -> new ArrayList<PluginJarInfo>())
                        .add(info);
            }
        }

        // A JAR is loaded when it is the newest provider of every identity it declares, which keeps
        // the decision consistent for a JAR that ships several plugins at once.
        Set<String> displaced = new HashSet<>();
        // One pair of duplicated JARs normally collides on several identities at once, for example
        // on an explicit Plugin-Id and on the entry-point set. Repeating the same advice per
        // identity would only bury it, so each set of JARs is reported once, under the first
        // identity it collided on. That one reads best, since entry-point identities come last.
        Set<List<String>> reported = new HashSet<>();
        String lib = new File(DumpRootResolverAgent.PROFILER_HOME).getAbsolutePath();
        for (Map.Entry<String, List<PluginJarInfo>> entry : byPluginId.entrySet()) {
            List<PluginJarInfo> jars = entry.getValue();
            if (jars.size() == 1) {
                continue;
            }
            PluginJarInfo winner = jars.get(0);
            List<String> jarPaths = new ArrayList<String>();
            for (PluginJarInfo jar : jars) {
                jarPaths.add(jar.jarPath);
                if (compareVersions(jar.version, winner.version) > 0) {
                    winner = jar;
                }
            }
            Collections.sort(jarPaths);
            boolean firstReport = reported.add(jarPaths);

            StringBuilder sb = new StringBuilder();
            sb.append("Profiler: plugin '").append(entry.getKey()).append("' is provided by several JARs. ")
                    .append("Only the newest one is loaded; remove the stale file(s) listed below:\n");
            for (PluginJarInfo jar : jars) {
                sb.append("  - ").append(jar.jarPath.replace(lib, "$esc"));
                if (jar.version != null) {
                    sb.append(" (version=").append(jar.version).append(")");
                }
                if (jar == winner) {
                    sb.append(" -- loaded\n");
                } else {
                    displaced.add(jar.jarPath);
                    sb.append(" -- skipped\n");
                }
            }
            if (firstReport) {
                logger.warning(sb.toString());
            } else {
                logger.fine(sb.toString());
            }
        }

        for (Map.Entry<String, List<PluginJarInfo>> entry : byPluginId.entrySet()) {
            for (PluginJarInfo jar : entry.getValue()) {
                if (!displaced.contains(jar.jarPath) && !result.contains(jar.jarPath)) {
                    result.add(jar.jarPath);
                }
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
                        info("Profiler: jar " + jarName + " was not loaded as a plugin");
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
