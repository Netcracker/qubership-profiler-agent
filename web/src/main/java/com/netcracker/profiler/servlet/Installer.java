package com.netcracker.profiler.servlet;

import com.netcracker.profiler.ServerNameResolver;
import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.ProfilerTransformerPlugin;
import com.netcracker.profiler.configuration.PropertyFacade;
import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.util.IOHelper;
import com.netcracker.profiler.util.VersionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class Installer implements ServletContextListener {
    private static final Logger log = LoggerFactory.getLogger(Installer.class);
    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");

    public Installer() {
        // Empty constructor as per listener spec
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
    }

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        install(servletContextEvent.getServletContext());
    }

    final static String backupSuffix = "." + System.currentTimeMillis();

    public void install(ServletContext context) {
        if (Boolean.getBoolean("com.netcracker.profiler.Installer.skip") &&
            !Boolean.getBoolean("com.netcracker.profiler.Installer.enable")) return;

        if (ServerNameResolver.SERVER_NAME == null || ServerNameResolver.SERVER_NAME.length() == 0) {
            log.info("Application server name is empty, installation is skipped\n" +
                    "Currently, weblogic (via weblogic.Name) and jboss (via jboss.server.name) servers are supported");
            return;
        }

        if (DumpRootResolver.PROFILER_HOME == null || DumpRootResolver.PROFILER_HOME.length() == 0) {
            log.error("Unable to extract profiler files since profiler.home is not known");
            return;
        }

        File home = new File(DumpRootResolver.PROFILER_HOME);

        byte[] ourVersion;
        try {
            ourVersion = readStream(context.getResourceAsStream("/version.txt"));
        } catch (IOException e) {
            log.error("Unable to get current profiler version", e);
            return;
        }
        if (ourVersion == null) {
            log.error("Unable to get current profiler version");
            return;
        }

        File existingVersion = new File(home, "version.txt");

        byte[] version = readFile(existingVersion, false);

        try {
            String nextVersion = new String(ourVersion, "UTF-8");
            String prevVersion = version != null ? new String(version, "UTF-8") : "";
            if (nextVersion.equals(prevVersion)) {
                log.info("Update is not required, since the installed version {} is up to date", prevVersion);
                return;
            }
            if (VersionUtils.naturalOrder(nextVersion, 5).compareTo(VersionUtils.naturalOrder(prevVersion, 5)) <= 0) {
                log.info("Current jar files are newer or the same version as the ones in war ({} >= {}). Will not perform downgrade. If you really need downgrade profiler version, extract older jar files manually", prevVersion, nextVersion);
                return;
            }
            log.info("Upgrading profiler version from {} to {}", prevVersion, nextVersion);
        } catch (UnsupportedEncodingException e) {
            log.error("UTF-8 encoding was not found for some reason", e);
        }


        Set<String> filesNotRequiringUpdate = new HashSet<String>();
        if (!backupFiles(context, home, filesNotRequiringUpdate))
            return;

        if (!extractBinaries(context, home, filesNotRequiringUpdate))
            return;

        updateWeblogicSetEnv();
    }

    @SuppressWarnings("resource")
    private boolean backupFiles(ServletContext context, File home, Set<String> filesNotRequiringUpdate) {
        log.info("Performing backups of existing files");
        final InputStream installerZip = getClass().getResourceAsStream("/WEB-INF/installer/installer.zip");
        if (installerZip == null) {
            log.error("Unable to fetch installer /WEB-INF/installer/installer.zip");
            return false;
        }

        ZipInputStream zis = new ZipInputStream(installerZip);
        try {
            ZipEntry ze;
            byte[] buf = new byte[1000];
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.isDirectory())
                    continue;

                final int len = (int) ze.getSize();
                if (buf.length < ze.getSize())
                    buf = new byte[len];

                DataInputStreamEx dis = new DataInputStreamEx(zis);
                dis.readFully(buf, 0, len);

                File dst = new File(home, ze.getName());
                if (!dst.toPath().normalize().startsWith(home.toPath())) {
                    throw new IllegalArgumentException("Bad zip entry: " + ze.getName());
                }
                if (!backupFile(dst, buf, 0, len, filesNotRequiringUpdate))
                    return false;
            }
            return true;
        } catch (IOException e) {
            log.error("Unable to process installer.zip", e);
            return false;
        } finally {
            try {
                zis.close();
            } catch (IOException e) {
                /**/
            }
        }
    }

    private boolean backupFile(File dst, byte[] newBytes, int offset, int length, Set<String> filesNotRequiringUpdate) {
        final String dstPath = dst.getAbsolutePath();
        if (!dst.exists()) {
            log.debug("File {} does not exist, no need to backup it", dstPath);
            return true;
        }
        final File parentFile = dst.getParentFile();
        if (parentFile == null) {
            log.info("Unable to resolve parent file for {}", dstPath);
            return false;
        }
        byte[] prevBytes = readFile(dst, true);

        if (firstElementsEqual(prevBytes, newBytes, offset, length)) {
            log.debug("Existing version of the file {} is the same as new one. No need to backup.", dstPath);
            filesNotRequiringUpdate.add(dstPath);
            return true;
        }

        final String prefix = dst.getName();
        String[] files = parentFile.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.length() > prefix.length() && name.startsWith(prefix);
            }
        });
        if (files != null && files.length > 0) {
            String lastBackup = files[0];
            for (int i = 1; i < files.length; i++)
                if (files[i].compareTo(lastBackup) > 0)
                    lastBackup = files[i];
            final File lastBackupFile = new File(parentFile, lastBackup);
            if (lastBackupFile.length() == dst.length()) {
                final byte[] lastBackupBytes = readFile(lastBackupFile, true);
                if (Arrays.equals(prevBytes, lastBackupBytes)) {
                    log.info("Last backup {} for file {} is the same as its current version. Do not creating one more version.", lastBackup, dstPath);
                    return true;
                }
            }
        }
        return writeFile(new File(parentFile, dst.getName() + backupSuffix), prevBytes);
    }

    private boolean firstElementsEqual(byte[] a, byte[] b, int offset, int length) {
        if (a.length != length) return false;
        if (a.length == b.length)
            return Arrays.equals(a, b);
        for (int i = 0; i < length; i++)
            if (a[i] != b[i])
                return false;
        return true;
    }

    private boolean writeFile(File dst, byte[] bytes) {
        return writeFile(dst, bytes, 0, bytes.length);
    }

    private boolean writeFile(File dst, byte[] bytes, int offset, int length) {
        if (length == 0 || bytes.length < length) {
            log.warn("Trying to create file of length {} with contents of {} bytes. Aborting installation to ensure data consistency.", length, bytes.length);
            return false;
        }
        FileOutputStream fos = null;
        final String dstAbsolutePath = dst.getAbsolutePath();
        try {
            File parentFile = dst.getParentFile();
            if (parentFile == null) {
                log.warn("Unable to retrieve parent file for {}", dstAbsolutePath);
                parentFile = dst.getAbsoluteFile();
                if (parentFile == null) {
                    log.error("Unable to retrieve absolute file for {}", dstAbsolutePath);
                    return false;
                }
                parentFile = parentFile.getParentFile();
                if (parentFile == null) {
                    log.error("Unable to retrieve parent file for {} via absolute file", dstAbsolutePath);
                    return false;
                }
            }
            if (!parentFile.exists() && !parentFile.mkdirs()) {
                log.error("Unable to create parent directory for file {}", dstAbsolutePath);
                return false;
            }

            fos = new FileOutputStream(dst);
            fos.write(bytes, offset, length);
            log.debug("Successfully written file {}", dstAbsolutePath);
            try {
                Runtime.getRuntime().exec(new String[]{"chmod", "a+rw", dstAbsolutePath});
            } catch (IOException e) {
                log.warn("Unable to grant rw access to public for file {}", dstAbsolutePath, e);
            }
            return true;
        } catch (FileNotFoundException e) {
            log.error("Unable to write file {}", dstAbsolutePath, e);
            return false;
        } catch (IOException e) {
            log.error("Unable to write file {}", dstAbsolutePath, e);
            return false;
        } finally {
            IOHelper.close(fos);
        }
    }

    @SuppressWarnings("resource")
    private boolean extractBinaries(ServletContext context, File home, Set<String> filesNotRequiringUpdate) {
        log.info("Extracting new files");
        final InputStream installerZip = getClass().getResourceAsStream("/WEB-INF/installer/installer.zip");
        if (installerZip == null) {
            log.error("Unable to fetch installer /WEB-INF/installer/installer.zip");
            return false;
        }

        ZipInputStream zis = new ZipInputStream(installerZip);
        try {
            ZipEntry ze;
            byte[] buf = new byte[1000];
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.isDirectory())
                    continue;

                File dst = new File(home, ze.getName());
                if (!dst.toPath().normalize().startsWith(home.toPath())) {
                    throw new IllegalArgumentException("Bad zip entry: " + ze.getName());
                }
                final String dstPath = dst.getAbsolutePath();
                if (filesNotRequiringUpdate.contains(dstPath)) {
                    log.info("Using existing file {} since its content is up to date", dstPath);
                    continue;
                }

                final int len = (int) ze.getSize();
                if (buf.length < ze.getSize())
                    buf = new byte[len];

                DataInputStreamEx dis = new DataInputStreamEx(zis);
                dis.readFully(buf, 0, len);

                if (!writeFile(dst, buf, 0, len))
                    return false;
            }
            return true;
        } catch (IOException e) {
            log.error("Unable to process installer.zip", e);
            return false;
        } finally {
            try {
                zis.close();
            } catch (IOException e) {
                /**/
            }
        }
    }

    byte[] readFile(File file, boolean failOnNotFound) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            return IOHelper.readFully(fis);
        } catch (FileNotFoundException e) {
            if (failOnNotFound)
                log.error("Unable to read file {}", file.getAbsolutePath(), e);
            else
                log.error("Unable to read file {}", file.getAbsolutePath());
            return null;
        } catch (IOException e) {
            log.error("Unable to read file {}", file.getAbsolutePath(), e);
            return null;
        } finally {
            IOHelper.close(fis);
        }
    }

    private void updateWeblogicSetEnv() {
        if (!isWeblogic()) {
            log.info("Looks like the application server is not Weblogic (weblogic.Server class does not resolve). Automatic installation is supported for Weblogic only.");
            return;
        }

        final String managementServer = getWeblogicClusterAdminName();
        if (managementServer != null) {
            log.info("This is a managed node (the admin server is {}). Adding javaagent option to managed nodes is not supported yet", managementServer);
            return;
        }

        final File setEnvFile = new File("setEnv.sh").getAbsoluteFile();
        final byte[] setEnvBytes = readFile(setEnvFile, true);
        if (setEnvBytes == null)
            return;

        byte[] setEnvBytesNew;
        try {
            setEnvBytesNew = getUpdatedSetEnv(setEnvBytes);
        } catch (IOException e) {
            log.error("Unable to generate new bytes for setEnv.sh", e);
            return;
        }
        if (setEnvBytesNew == null) {
            log.error("Unable to generate new bytes for setEnv.sh");
            return;
        }

        if (Arrays.equals(setEnvBytes, setEnvBytesNew)) {
            log.info("Current setEnv.sh is up to date");
            return;
        }

        Set<String> updateNotRequired = new HashSet<String>();
        if (!backupFile(setEnvFile, setEnvBytesNew, 0, setEnvBytesNew.length, updateNotRequired))
            return;
        if (updateNotRequired.size() == 1) {
            log.info("Current setEnv.sh is up to date");
            return;
        }

        writeFile(setEnvFile, setEnvBytesNew);
    }

    private byte[] getUpdatedSetEnv(byte[] setEnv) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(setEnv.length + 500);
        Writer setEnvNew = new OutputStreamWriter(os);

        BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(setEnv)));
        for (String line; (line = in.readLine()) != null; ) {
            if (line.contains("/profiler/lib/")
              || line.contains("/profiler/bin/")
              || line.contains("/execution-statistics-collector/lib/")) {
                log.debug("Ignoring line in setEnv.sh <<{}>> since it looks like profiler generated one", line);
                continue;
            }
            setEnvNew.write(line);
            setEnvNew.write('\n');
        }

        setEnvNew.write(getJavaOptionsForSetEnv());
        setEnvNew.write('\n');
        setEnvNew.flush();
        return os.toByteArray();
    }

    private static boolean isWeblogic() {
        try {
            Class.forName("weblogic.Server");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static String getWeblogicClusterAdminName() {
        return System.getProperty("weblogic.management.server");
    }

    private static String getJavaOptionsForSetEnv() {
        if (IS_WINDOWS)
            return "set JAVA_OPTIONS=%JAVA_OPTIONS% " + getJavaOption();
        return "JAVA_OPTIONS=\"${JAVA_OPTIONS} " + getJavaOption() + "\"";
    }

    private static String getJavaOption() {
        String javaAgentOption;

        final File workingDir = new File(".").getAbsoluteFile().getParentFile();

        String javaVersion = System.getProperty("java.version", "1.5");
        if (javaVersion.startsWith("1.4"))
            javaAgentOption = "-Xbootclasspath/p:" + getProfilerLibFile(new File(workingDir, "applications"), "boot.jar") + " -Xbootclasspath/a:" + getProfilerLibFile(new File(workingDir, "applications"), "profiler-boot.jar");
        else
            javaAgentOption = "-javaagent:" + getProfilerLibFile(new File("applications"), "agent.jar");
        return javaAgentOption;
    }

    private static File getProfilerLibFile(File root, String file) {
        return new File(new File(new File(root, "execution-statistics-collector"), "lib"), file);
    }

    private byte[] readStream(InputStream is) throws IOException {
        if (is == null) return null;

        try {
            return IOHelper.readFully(is);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                /**/
            }
        }
    }

    public static String getNonActiveProfilerWarning() {
        if (PropertyFacade.getProperty("com.netcracker.profiler.skip.nonactive", null) != null) return null;

        try {
            final ProfilerTransformerPlugin transformer = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);
            if (transformer != null) return null;

            if (Bootstrap.class.getClassLoader() != ClassLoader.getSystemClassLoader().getParent())
                return getRecommendations(new StringBuffer("Bootstrap class is not loaded with boot classloader.</p><p>Please ensure you did <b>not</b> put profiler jars to the classpath</p><p>")).toString();

            return getRecommendations(new StringBuffer("Agent is not completely loaded: <b>profiling transformer is missing</b> for some reason (e.g. the profiler/bin directory does not contain all the required jars).</p>\n" +
                    "<p>" +
                    "<p>Please, remove profiler/version.txt file, redeploy profiler web application and then restart the server.</p><p>")).toString();
        } catch (Throwable t) {
            /**/
        }

        return getRecommendations(new StringBuffer("Java agent is not installed properly.</p><p>")).toString();
    }

    private static StringBuffer getRecommendations(StringBuffer sb) {
        sb.append("Please, add the following ");
        if (getWeblogicClusterAdminName() != null) {
            sb.append("java options to the startup arguments of each Weblogic node (clust1, clust2, etc): <code>").append(getJavaOption()).append("</code>");
        } else if (isWeblogic()) {
            sb.append("line to the ");
            if (IS_WINDOWS)
                sb.append("setEnv.cmd");
            else
                sb.append("setEnv.sh");
            sb.append(" file: <code>").append(getJavaOptionsForSetEnv()).append("</code> and restart the server");
        } else {
            sb.append("options to the startup arguments of the server: <code>").append(getJavaOption()).append("</code> and restart it");
        }
        return sb;
    }

    public static List<String> getStartupArgumentRecommendations() {
        List<String> args;
        try {
            args = getStartupArguments();
        } catch (Throwable t) {
            log.warn("Unable to get startup arguments. Some misconfiguration might be unnoticed", t);
            return Collections.emptyList();
        }
        if (args == null || args.isEmpty())
            return Collections.emptyList();

        List<String> result = new ArrayList<String>();
        boolean xInt = "NONE".equals(System.getProperty("java.compiler"));
        boolean debug = false;
        boolean gcLog = false;
        boolean heapDumpOnOOM = false;
        boolean isProfilerWar = false;
        boolean hasAggressiveOpts = false;
        for (String arg : args) {
            if (arg.startsWith("-Xdebug") || arg.startsWith("-Xrunjdwp"))
                debug = true;
            else if (arg.startsWith("-Xint"))
                xInt = true;
            else if (arg.startsWith("-XX:+HeapDumpOnOutOfMemoryError"))
                heapDumpOnOOM = true;
            else if (arg.startsWith("-Xloggc"))
                gcLog = true;
            else if (arg.startsWith("profiler-java15.war"))
                isProfilerWar = true;
            else if (arg.startsWith("-XX:+AggressiveOpts"))
                hasAggressiveOpts = true;
        }
        if (debug)
            result.add("Debug mode is ON, so java VM is running in non-optimized mode. Consider non-debug mode for performance measurements (remove -Xdebug, -Xrunjdwp, and -Djava.compiler=NONE options).");
        if (xInt)
            result.add("Java JIT compiler is disabled. JIT should be enabled for any real measurement (remove -Xint, and -Djava.compiler=NONE options).");
        String javaVendor = System.getProperty("java.vendor");
        if (!isProfilerWar && ("Sun Microsystems Inc.".equals(javaVendor) || "Oracle Corporation".equals(javaVendor))
                && (!heapDumpOnOOM || !gcLog)
                ) {
            StringBuilder msg = new StringBuilder("Add");
            if (!heapDumpOnOOM)
                msg.append(" -XX:+HeapDumpOnOutOfMemoryError");
            if (!gcLog) {
                if (msg.length() > 5)
                    msg.append(", ");
                msg.append(" -Xloggc");
            }
            msg.append(", and other gc logging options to enable troubleshooting of out of memory/memory leak cases");
            result.add(msg.toString());
        }
        if (hasAggressiveOpts) {
            result.add("Please, consider removing -XX:+AggressiveOpts as it might affect functional behaviour");
        }
        return result;
    }

    public static List<String> getStartupArguments() {
        RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
        return RuntimemxBean.getInputArguments();
    }
}
