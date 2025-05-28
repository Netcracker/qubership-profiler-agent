package org.qubership.profiler;

import org.apache.catalina.Context;
import org.apache.catalina.Host;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;

import java.awt.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;

public class WARLauncher {
    //something unique
    public static final File TEMP_DIR = new File(System.getProperty("java.io.tmpdir"), "ncescclilibs_5a65ba94-f9e7-11ea-adc1-0242ac120002");
    public static final File TOMCAT_BASE_DIR = new File(TEMP_DIR, "tomcat");
    public static final File TOMCAT_DOC_BASE = new File(TEMP_DIR, "tomcat-docBase");

    private static final int DEFAULT_PORT = 8180;
    private static final String PORT_PROPERTY_KEY = "server.port";

    public static String PATH_TO_WAR_FILE;

    private static LoggerProxy logger;
    private static URLClassLoader nestedJarsClassLoader;

    @PostConstruct
    public void start(){

    }

    private static void ensureFolder(File folder) {
        if(!folder.exists() && !folder.mkdirs()){
            throw new RuntimeException("Failed to create a temp folder " + folder.getAbsolutePath());
        }
        if(folder.exists() && !folder.isDirectory()) {
            throw new RuntimeException(folder.getAbsolutePath() + " is not a directory");
        }
    }

    private static boolean attemptCLI(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, ClassNotFoundException {
        Class cli = nestedJarsClassLoader.loadClass("org.qubership.profiler.cli.Main");
        if(args.length > 0 && "cli".equals(args[0])){
            logger.info("executing the cli");
            cli.getMethod("main", String[].class).invoke(null, (Object)Arrays.copyOfRange(args, 1, args.length));
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, LifecycleException {
        URL url = WARLauncher.class.getProtectionDomain().getCodeSource().getLocation();
        File warFile = new File(url.toURI());
        nestedJarsClassLoader = createNestedJarsClassloader(warFile);
        PATH_TO_WAR_FILE = warFile.getPath();

        try{
            logger = new LoggerProxy(WARLauncher.class, nestedJarsClassLoader);
            System.setProperty("profiler_local_start_mode", String.valueOf(!Boolean.getBoolean("profiler_standalone_mode")));
            String profilerHome = getProfilerHomeFromDumpRootResolver();
            File propertiesFile = new File(new File(profilerHome), "config/tomcat/application.properties");
            Properties applicationProperties = loadPropertiesFromFile(propertiesFile);

            if(attemptCLI(args)){
                logger.info("CLI has been executed");
                return;
            }

            System.setProperty("org.qubership.profiler.Installer.skip", "true");
            System.setProperty("org.qubership.profiler.skip.nonactive", "true");


            Map<String, String> tomcatArgs = new HashMap<String, String>();
            logger.info("warFile.getAbsolutePath() = {}", warFile.getAbsolutePath());
            tomcatArgs.put("warfile", warFile.getAbsolutePath());

            boolean dumpRootset = false;
            String logLevel = null;
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("--httpPort")) {
                    i++;
                    if (i == args.length) {
                        logger.error("Please specify http port number after --httpPort");
                        System.exit(-1);
                    }
                    try {
                        int portNumber = Integer.parseInt(args[i]);
                        tomcatArgs.put("httpPort", args[i]);
                    } catch (NumberFormatException e) {
                        logger.error("Unable to parse '{}' as http port number ", args[i]);
                        System.exit(-1);
                    }
                    continue;
                }
                if (args[i].equals("--httpListenAddress")) {
                    i++;
                    if (i == args.length) {
                        logger.error("Please specify http listen address after --httpListenAddress");
                        System.exit(-1);
                    }
                    tomcatArgs.put("httpListenAddress", args[i]);
                    continue;
                }
                if (args[i].equals("--trace")) {
                    i++;
                    logLevel = "trace";
                    continue;
                } else if (i + 1 < args.length && args[i].equals("-v") && args[i + 1].equals("-v")) {
                    i += 2;
                    logLevel = "trace";
                    continue;
                } else if (args[i].equals("--debug") || args[i].equals("-v")) {
                    i++;
                    logLevel = "debug";
                    continue;
                }
                if (args[i].equals("-h") || args[i].equals("--help") || args[i].equals("/h") ||
                        args[i].equals("-?") || args[i].equals("/?")) {
                    usage(warFile.getName());
                    return;
                }
                if (args[i].startsWith("-")) {
                    logger.error("Unrecognized option: {}", args[i]);
                    usage(warFile.getName());
                    return;
                }
                if (i < args.length - 1) {
                    logger.error("Too many options provided: {} ...", args[i]);
                    logger.error("Expecting just one option with dump folder location of [execution-statistics-collector/dump]");
                    usage(warFile.getName());
                    return;
                }
                if (i == args.length - 1) {
                    logger.info("Will read profiler logs from {}", args[i]);
                    String dumpRoot = args[i] + "/default";
                    System.setProperty("profiler.dump", dumpRoot);
                    setDumpRootOnDumpRootResolver(dumpRoot);
                    dumpRootset = true;
                }
            }
            if (logLevel != null) {
                logger.info("logLevel = {}", logLevel);
                System.setProperty("esc.web.log.level", logLevel);
            }
            if (!dumpRootset) {
                String dumpRoot = getDumpRootFromDumpRootResolver();
                if(dumpRoot == null) {
                    usage(warFile.getName());
                } else {
                    logger.info("Will read profiler logs from {}", dumpRoot);
                }
            }

            logger.info("Starting profiler results viewer...");

            String hostName = (String) tomcatArgs.get("httpListenAddress");
            if (hostName == null)
                hostName = InetAddress.getLocalHost().getCanonicalHostName();

            String portStr = (String) tomcatArgs.get("httpPort");
            int port;
            if (portStr != null) {
                port = Integer.parseInt(portStr);
            } else if(applicationProperties.containsKey(PORT_PROPERTY_KEY)) {
                portStr = applicationProperties.getProperty(PORT_PROPERTY_KEY);
                port = Integer.parseInt(portStr);
            } else {
                port = DEFAULT_PORT;
            }

            String contextPath = "";

            ensureFolder(TOMCAT_BASE_DIR);
            String tomcatBaseDir = TOMCAT_BASE_DIR.getAbsolutePath();
            copyTomcatConfigsToTomcatBaseDir(new File(tomcatBaseDir));
            ensureFolder(TOMCAT_DOC_BASE);
            String contextDocBase = TOMCAT_DOC_BASE.getAbsolutePath();

            Tomcat tomcat = new Tomcat();
            tomcat.setBaseDir(tomcatBaseDir);

            tomcat.setPort(port);
            // In fact, getConnector **creates** a connector, so we need this call
            tomcat.getConnector();
            tomcat.setHostname(hostName);

            Host host = tomcat.getHost();
            Context context = tomcat.addWebapp(host, contextPath, contextDocBase);
            context.setParentClassLoader(nestedJarsClassLoader);

            File contextFile = new File(new File(profilerHome), "config/tomcat/context.xml");
            if(contextFile.exists()) {
                context.setConfigFile(contextFile.toURI().toURL());
            };

            context.addLifecycleListener(new WebappMountListener());
            tomcat.start();

            if(!contextFile.exists()) {
                removeSecurityConstraints(context);
            };

            URI uri = new URI("http://" + hostName + ":" + port);
            try {
                logger.info("Trying to open {} in default browser", uri.toString());
                if(openWindowsNativeFailed(uri)){
                    Class.forName("java.awt.Desktop");
                    openBrowser(uri);
                }
            } catch (ClassNotFoundException e) {
                logger.warn("Please, navigate your browser to {}", uri.toString());
            }

            tomcat.getServer().await();
        } catch (Throwable t) {
            if(logger != null) {
                logger.error("Error happened: ", t);
            } else {
                t.printStackTrace();
            }
        }
    }

    private static void usage(String warFile) {
        logger.info("Profiler results viewer");
        logger.info(" Usage: java -jar {} [options] [path/to/profiler/dump]", warFile);
        logger.info("   path/to/profiler/dump is a path to the profiler/dump folder. Default is profiler/dump");
        logger.info(" Options:");
        logger.info("   --httpPort          set the http listening port. Default is 8180");
        logger.info("   --httpListenAddress set the http listening address. Default is all interfaces");
        logger.info("   --debug             activate debug log level for web application");
        logger.info("   --trace             activate trace log level for web application");

    }

    public static void openBrowser(URI uri) {
        if (!Desktop.isDesktopSupported()) {
            logger.warn("Desktop is not supported, please open {} manually", uri);
            return;
        }

        Desktop desktop = Desktop.getDesktop();

        if (!desktop.isSupported(Desktop.Action.BROWSE)) {
            logger.warn("Desktop doesn't support the browse action, please open {} manually", uri);
            return;
        }

        try {
            desktop.browse(uri);
        } catch (Exception e) {
            logger.warn("Unable to start browser, please open {} manually", uri);
            e.printStackTrace();
        }
    }

    private static boolean openWindowsNativeFailed(URI uri) {
        Runtime runtime = Runtime.getRuntime();
        try {
            runtime.exec("rundll32 url.dll,FileProtocolHandler " + uri);
            return false;
        } catch (IOException e) {
            //ignore
        }
        return true;
    }

    private static void copyTomcatConfigsToTomcatBaseDir(File tomcatBaseDir) throws IOException {
        File tomcatConfigFolder = new File("applications/execution-statistics-collector/config/tomcat");
        if(tomcatConfigFolder.exists()) {
            for(File file : tomcatConfigFolder.listFiles()) {
                Files.copy(file.toPath(), new File(tomcatBaseDir, file.getName()).toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static void removeSecurityConstraints(Context context) {
        for(SecurityConstraint sc : context.findConstraints()) {
            context.removeConstraint(sc);
        }
    }

    private static URLClassLoader createNestedJarsClassloader(File warFile) throws IOException {
        File toUnzipTo = TEMP_DIR;
        ensureFolder(toUnzipTo);

        ZipFile zipFile = new ZipFile(warFile);
        List<URL> jars = new ArrayList<>();
        //5 mb buffer for classes
        ByteArrayOutputStream bout = new ByteArrayOutputStream(5*1024*1024);
        ZipOutputStream zout = new ZipOutputStream(bout);
        boolean extractedClasses = false;

        for(Enumeration<? extends ZipEntry> e = zipFile.entries(); e.hasMoreElements();){
            ZipEntry ze = e.nextElement();
            if(ze.getName().startsWith("WEB-INF/classes/") && !ze.isDirectory()){
                String className = ze.getName().substring("WEB-INF/classes/".length());
                extractedClasses = true;
                zout.putNextEntry(new ZipEntry(className));
                try(InputStream zin = zipFile.getInputStream(ze)){
                    byte[] buf = new byte[1024];
                    int len;
                    while((len=zin.read(buf)) > 0){
                        zout.write(buf, 0, len);
                    }
                }
                continue;
            }
            if(!ze.getName().endsWith(".jar")){
                continue;
            }
            File target = new File(toUnzipTo, ze.getCrc() + ".jar");
            jars.add(target.toURI().toURL());
            if(target.exists()){
                continue;
            }

            ensureFolder(target.getParentFile());
            try(InputStream zin = zipFile.getInputStream(ze)){
                try(OutputStream out = new BufferedOutputStream(new FileOutputStream(target))){
                    byte[] buf = new byte[1024];
                    int len;
                    while((len=zin.read(buf)) > 0){
                        out.write(buf, 0, len);
                    }
                }
            }
        }

        if(extractedClasses){
            zout.close();
            int hashCode = ByteBuffer.wrap(bout.toByteArray(), 0, bout.size()).hashCode();
            File warClassesJar = new File(toUnzipTo, "classes" + hashCode + ".jar");
            if(!warClassesJar.exists()){
                try(OutputStream out = new BufferedOutputStream(new FileOutputStream(warClassesJar))){
                    bout.writeTo(out);
                }
            }
            jars.add(warClassesJar.toURI().toURL());
        }

        return new URLClassLoader(jars.toArray(new URL[jars.size()]), WARLauncher.class.getClassLoader());
    }

    private static String getDumpRootFromDumpRootResolver() {
        try {
            return (String)nestedJarsClassLoader.loadClass("org.qubership.profiler.dump.DumpRootResolver").getDeclaredField("dumpRoot").get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setDumpRootOnDumpRootResolver(String dumpRoot) {
        try {
            nestedJarsClassLoader.loadClass("org.qubership.profiler.dump.DumpRootResolver").getDeclaredField("dumpRoot").set(null, dumpRoot);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getProfilerHomeFromDumpRootResolver() {
        try {
            return (String)nestedJarsClassLoader.loadClass("org.qubership.profiler.dump.DumpRootResolver").getDeclaredField("PROFILER_HOME").get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties loadPropertiesFromFile(File file) {
        Properties properties = new Properties();
        try(InputStream in = new FileInputStream(file)) {
            properties.load(in);
        } catch (Exception e) {
            //Do nothing
        }
        return properties;
    }
}
