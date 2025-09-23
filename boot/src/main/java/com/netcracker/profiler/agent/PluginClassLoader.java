package com.netcracker.profiler.agent;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Level;

public class PluginClassLoader extends URLClassLoader {
    public static final boolean DEBUG_PRELOADING = Boolean.getBoolean(PluginClassLoader.class.getName() + ".debug_preloading");
    private static final ESCLogger logger = ESCLogger.getLogger(PluginClassLoader.class, (DEBUG_PRELOADING ? Level.FINE : ESCLogger.ESC_LOG_LEVEL));
    String[] entryPoints;
    private final String preloadList;

    public PluginClassLoader(ClassLoader parent, URL[] urls, String[] entryPoints, String preloadList) {
        // Use null parent class loader to avoid loading classes from system class path.
        // That enables only bootstrap classes to be visible to profiler
        super(urls, parent);
        this.entryPoints = entryPoints;
        this.preloadList = preloadList;
    }

    private void preload(String listName) {
        URL url = findResource(listName);
        if (url == null) return;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
            int i = 0;
            for (String s; (s = br.readLine()) != null; i++) {
                try {
                    if (DEBUG_PRELOADING) {
                        logger.fine("Preloading " + s);
                    }
                    Class.forName(s, false, this);
                } catch (ClassNotFoundException e) {
                    if (DEBUG_PRELOADING)
                        logger.log(Level.FINE, "", e);
                } catch (NoClassDefFoundError e) {
                    if (DEBUG_PRELOADING)
                        logger.log(Level.FINE, "", e);
                } catch (Throwable e) {
                    if (DEBUG_PRELOADING)
                        logger.log(Level.FINE, "", e);
                }
            }
            Bootstrap.info("Profiler: Preloaded " + i + " classes from " + getURLs()[0]);
        } catch (IOException e) {
            logger.severe("Profiler: Unable to preload classes from " + url + ". This should not be critical problem, but it may result in deadlocks", e);
        } finally {
            if (br != null) try {
                br.close();
            } catch (IOException e) {
                /**/
            }
        }
    }

    public static PluginClassLoader newInstance(String jarName) throws IOException, URISyntaxException {
        Attributes attrs = getManifestAttributes(jarName);
        final String value = attrs.getValue("Entry-Points");
        if (value == null || value.length() == 0) return null;
        String[] entryPoints = value.split("\\s+");
        String preloadList = attrs.getValue("Preload-Classes-List");
        String dependencies = attrs.getValue("Esc-Dependencies");
        ClassLoader parentLoader = null;
        if ("instrumenter".equals(dependencies)) {
            ProfilerTransformerPlugin plugin = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);
            if (plugin == null) {
                logger.severe("Plugin " + jarName + " requires instrumenter, however it was not found on the classpath. Please ensure /lib/runtime.jar exists and can be loaded.");
            } else parentLoader = plugin.getClass().getClassLoader();
        }
        return new PluginClassLoader(parentLoader, new URL[]{new File(jarName).toURI().toURL()}, entryPoints, preloadList);
    }

    private static Attributes getManifestAttributes(String jarName) throws IOException {
        final JarFile jar = new JarFile(jarName);
        final Manifest manifest = jar.getManifest();
        jar.close();
        return manifest.getMainAttributes();
    }

    public List<Object> startPlugin() throws ClassNotFoundException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, InstantiationException {
        if (preloadList != null && preloadList.length() > 0)
            preload(preloadList);
        List<Object> res = new ArrayList<Object>();
        for (String entryPoint : entryPoints) {
            Object impl = loadClass(entryPoint).newInstance();
            res.add(impl);
        }
        return res;
    }
}
