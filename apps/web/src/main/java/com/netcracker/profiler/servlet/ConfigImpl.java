package com.netcracker.profiler.servlet;

import com.netcracker.profiler.agent.*;
import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.io.JSHelper;
import com.netcracker.profiler.servlet.util.DumperStatusProvider;
import com.netcracker.profiler.util.StringUtils;
import com.netcracker.profiler.util.ThrowableHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URLEncoder;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ConfigImpl {
    public static final Logger log = LoggerFactory.getLogger(ConfigImpl.class);
    public final static String CONFIG_FILTERS_FILE = "_config.filters.xml";
    public final static String CONFIG_FILTERS_OVERRIDE_FILE = "_config.filters.override.xml";

    void processConfigurationReload(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String configPath = request.getParameter("config");

        Throwable result = null;
        try {
            updateConfigPath(configPath);
            String now = request.getParameter("now");
            if (now != null && Boolean.valueOf(now))
                reloadConfig(null); // Use current configuration file
        } catch (Throwable t) {
            result = t;
        }

        showReloadStatus(request, response, result);
    }

    void updateConfigPath(String configPath) throws IOException {
        final String currentConfig = getCurrentConfigFile();

        File config = new File(currentConfig);
        File configDir = config.getAbsoluteFile().getParentFile();
        File newConfig = new File(configDir, configPath);
        if (!newConfig.exists())
            throw new IOException("Unable to use " + configPath + " as new config file since it does not exist");
        if(!newConfig.toPath().normalize().startsWith(configDir.toPath())) {
            throw new IllegalArgumentException("Access denied. The path is outside of config folder.");
        }
        File configFilters = new File(configDir, CONFIG_FILTERS_OVERRIDE_FILE);
        if (!configFilters.exists())
            configFilters = new File(configDir, CONFIG_FILTERS_FILE);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder;
        OutputStream out = null;
        try {
            builder = dbf.newDocumentBuilder();
            final Document doc = builder.parse(configFilters);
            Element rule = null;

            final NodeList list = doc.getElementsByTagName("rule");
            if (list != null)
                for (int i = 0, len = list.getLength(); i < len; i++) {
                    Node node = list.item(i);
                    if (!(node instanceof Element)) continue;
                    Element el = (Element) node;
                    if (!"filter-rule".equals(el.getAttribute("id"))) continue;
                    rule = el;
                    break;
                }

            if (rule == null)
                throw new IllegalStateException("Unable to update " + configFilters.getAbsolutePath() + " file since rule with id of filter-rule was not found");

            rule.setAttribute("src", configPath);

            final TransformerFactory trf = TransformerFactory.newInstance();
            final Transformer tr = trf.newTransformer();
            tr.setOutputProperty(OutputKeys.INDENT, "yes");

            out = new BufferedOutputStream(new FileOutputStream(new File(configDir, CONFIG_FILTERS_OVERRIDE_FILE)));
            tr.transform(new DOMSource(doc), new StreamResult(out));

        } catch (ParserConfigurationException e) {
            log.warn("Unable to parse file {}", configFilters.getAbsolutePath(), e);
        } catch (SAXException e) {
            log.warn("Unable to parse file {}", configFilters.getAbsolutePath(), e);
        } catch (IOException e) {
            log.warn("Unable to parse file {}", configFilters.getAbsolutePath(), e);
        } catch (TransformerConfigurationException e) {
            log.warn("Unable to write file {}", configFilters.getAbsolutePath(), e);
        } catch (TransformerException e) {
            log.warn("Unable to write file {}", configFilters.getAbsolutePath(), e);
        } finally {
            if (out != null)
                try {
                    out.close();
                } catch (IOException e) {
                    /* */
                }
        }
    }

    private String getCurrentFiletringConfig(String configPath) {
        File config = new File(configPath);
        File configDir = config.getAbsoluteFile().getParentFile();
        File configFilters = new File(configDir, CONFIG_FILTERS_OVERRIDE_FILE);
        if (!configFilters.exists())
            configFilters = new File(configDir, CONFIG_FILTERS_FILE);

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder;
        try {
            builder = dbf.newDocumentBuilder();
            final Document doc = builder.parse(configFilters);
            final NodeList list = doc.getElementsByTagName("rule");
            if (list == null) return "";
            for (int i = 0, len = list.getLength(); i < len; i++) {
                Node node = list.item(i);
                if (!(node instanceof Element)) continue;
                Element el = (Element) node;
                if (!"filter-rule".equals(el.getAttribute("id"))) continue;
                return el.getAttribute("src");
            }
        } catch (ParserConfigurationException e) {
            log.warn("Unable to parse file {}", configFilters.getAbsolutePath(), e);
        } catch (SAXException e) {
            log.warn("Unable to parse file {}", configFilters.getAbsolutePath(), e);
        } catch (IOException e) {
            log.warn("Unable to parse file {}", configFilters.getAbsolutePath(), e);
        }
        return "";
    }

    private String getCurrentConfigFile() {
        try {
            final ProfilerTransformerPlugin_01 tr = getTransformer();
            final ReloadStatus status = tr.getReloadStatus();
            return status.getConfigPath();
        } catch (Throwable t) {
            log.info("Unable to get config root. Looks like Profiler agent is not running", t);
        }
        return DumpRootResolver.CONFIG_FILE;
    }

    void showReloadStatus(HttpServletRequest request, HttpServletResponse response, Throwable overriden) throws ServletException, IOException {
        response.setContentType("application/x-javascript; charset=utf-8");
        final PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "utf-8")), false);
        String id = request.getParameter("id");
        if(!StringUtils.isNumeric(id)) {
            out.print("alert('Wrong id passed to /config/reload_status');");
            out.flush();
            return;
        }
        out.print("dataReceived");
        out.print('(');
        out.print(id);
        out.print(", ");
        StringWriter sw = new StringWriter();
        if (overriden == null) {
            try {
                final ProfilerTransformerPlugin_01 tr = getTransformer();
                final ReloadStatus status = tr.getReloadStatus();
                sw.append('[');
                sw.append(Boolean.toString(status.isDone())).append(',');
                sw.append(Integer.toString(status.getTotalCount())).append(',');
                sw.append(Integer.toString(status.getSuccessCount())).append(',');
                sw.append(Integer.toString(status.getErrorCount())).append(",'");
                String configPath = status.getConfigPath();
                configPath = relativeToCWD(configPath);
                JSHelper.escapeJS(sw, configPath);
                sw.append("','");
                JSHelper.escapeJS(sw, getCurrentFiletringConfig(status.getConfigPath()));
                sw.append("','");
                JSHelper.escapeJS(sw, status.getMessage());
                sw.append("']");
            } catch (Throwable t) {
                overriden = t;
            }
        }
        if (overriden == null)
            out.print(sw.toString());
        else {
            String configPath = DumpRootResolver.CONFIG_FILE;
            out.print("[1,1,0,1,'");
            JSHelper.escapeJS(out, configPath);
            out.append("','");
            JSHelper.escapeJS(out, getCurrentFiletringConfig(configPath));
            out.print("','Reload failed: ");
            JSHelper.escapeJS(out, ThrowableHelper.throwableToString(overriden));
            out.print("']");
        }

        out.print(");");
        out.flush();
    }

    private String relativeToCWD(String path) {
        if (path != null) {
            File cwd = new File(".").getAbsoluteFile();
            if (cwd != null) cwd = cwd.getParentFile();
            if (cwd != null) {
                String cwdPath = cwd.getAbsolutePath();
                if (cwdPath != null && path.startsWith(cwdPath))
                    path = path.substring(cwdPath.length() + 1);
            }
        }
        return path;
    }

    private void reloadConfig(String configPath) throws IOException, SAXException, ParserConfigurationException {
        ProfilerTransformerPlugin_01 transformer = getTransformer();
        if (transformer == null) return;
        transformer.reloadConfiguration(configPath);
    }

    public ProfilerTransformerPlugin_01 getTransformer() {
        final ProfilerTransformerPlugin transformerPlugin = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);
        if (transformerPlugin == null || !(transformerPlugin instanceof ProfilerTransformerPlugin_01))
            throw new IllegalStateException("Installed version of profiler does not support configuration reloading");
        return (ProfilerTransformerPlugin_01) transformerPlugin;
    }

    private DumperPlugin_02 getDumper() {
        final DumperPlugin dumperPlugin = Bootstrap.getPlugin(DumperPlugin.class);
        if (dumperPlugin == null || !(dumperPlugin instanceof DumperPlugin_02))
            throw new IllegalStateException("Installed version of dumper does not support on the fly management");
        return (DumperPlugin_02) dumperPlugin;
    }

    void showDumperStatus(HttpServletRequest request, HttpServletResponse response, Throwable overriden) throws IOException {
        response.setContentType("application/x-javascript; charset=utf-8");
        final PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "utf-8")), false);
        String callback = request.getParameter("callback");
        if (callback == null) callback = "dataReceived";
        callback = URLEncoder.encode(callback, "UTF-8");
        out.print(callback);
        out.print('(');
        out.print(URLEncoder.encode(request.getParameter("id"), "UTF-8"));
        out.print(", ");
        StringWriter sw = new StringWriter();
        if (overriden == null) {
            try {
                computeDumperStatus(sw);
            } catch (Throwable t) {
                overriden = t;
            }
        }
        if (overriden == null)
            out.print(sw.toString());
        else {
            out.print("['Unable to request dumper status: ");
            JSHelper.escapeJS(out, ThrowableHelper.throwableToString(overriden));
            out.print("']");
        }

        out.print(");");
        out.flush();
    }

    private void computeDumperStatus(StringWriter sw) {
        final DumperStatusProvider dumper = DumperStatusProvider.INSTANCE;
        dumper.update();
        sw.append('[');
        sw.append(Boolean.toString(dumper.isStarted));
        sw.append(',');
        sw.append(Long.toString(dumper.activeTime));
        sw.append(',');
        sw.append(Integer.toString(dumper.numberOfRestarts));
        sw.append(',');
        sw.append(Long.toString(dumper.writeTime));
        sw.append(',');
        sw.append(Long.toString(dumper.writtenBytes));
        sw.append(',');
        sw.append(Long.toString(dumper.writtenRecords));
        sw.append(',');
        sw.append('"');
        final File root = dumper.currentRoot;
        if (root != null) {
            final String rootPath = relativeToCWD(root.getAbsolutePath());
            try {
                JSHelper.escapeJS(sw, rootPath);
            } catch (IOException e) {
                // Can not happen since we are writing to StringWriter
            }
        }
        sw.append('"');
        sw.append(',');
        sw.append(Long.toString(dumper.cpuTime));
        sw.append(',');
        sw.append(Long.toString(dumper.bytesAllocated));
        sw.append(',');
        sw.append(Long.toString(dumper.fileRead));
        sw.append(',');
        sw.append(Long.toString(dumper.archiveSize));
        sw.append(']');
    }

    void stopDumper(HttpServletRequest request, HttpServletResponse response, Throwable overriden) throws IOException {
        try {
            final DumperPlugin_02 dumper = getDumper();
            dumper.stop(true);
        } catch (Throwable t) {
            overriden = t;
        }
        showDumperStatus(request, response, overriden);
    }

    void startDumper(HttpServletRequest request, HttpServletResponse response, Throwable overriden) throws IOException {
        try {
            final DumperPlugin_02 dumper = getDumper();
            dumper.start();
        } catch (Throwable t) {
            overriden = t;
        }
        showDumperStatus(request, response, overriden);
    }

    void rescanDumpFiles(HttpServletRequest request, HttpServletResponse response, Throwable overriden) throws IOException {
        try {
            final DumperPlugin_02 dumper = getDumper();
            if (dumper instanceof DumperPlugin_08) {
                DumperPlugin_08 d = (DumperPlugin_08) dumper;
                d.forceRescanDumpDir();
            }
        } catch (Throwable t) {
            overriden = t;
        }
        showDumperStatus(request, response, overriden);
    }
}
