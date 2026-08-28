package com.netcracker.profiler.configuration;

import com.netcracker.profiler.agent.*;
import com.netcracker.profiler.agent.plugins.ConfigurationSPI;
import com.netcracker.profiler.configuration.callfilters.*;
import com.netcracker.profiler.configuration.callfilters.metrics.*;
import com.netcracker.profiler.configuration.callfilters.metrics.condition.*;
import com.netcracker.profiler.configuration.callfilters.params.FilterOperatorContains;
import com.netcracker.profiler.configuration.callfilters.params.FilterOperatorEndsWith;
import com.netcracker.profiler.configuration.callfilters.params.FilterOperatorExact;
import com.netcracker.profiler.configuration.callfilters.params.FilterOperatorStartsWith;
import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.instrument.custom.MethodAcceptor;
import com.netcracker.profiler.instrument.custom.MethodAcceptorsList;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;
import com.netcracker.profiler.instrument.custom.util.*;
import com.netcracker.profiler.instrument.enhancement.EnhancementRegistryImpl;
import com.netcracker.profiler.instrument.enhancement.EnhancerPlugin;
import com.netcracker.profiler.io.DurationParser;
import com.netcracker.profiler.io.SizeParser;
import com.netcracker.profiler.util.StringUtils;
import com.netcracker.profiler.util.XMLHelper;

import org.objectweb.asm.Opcodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class ConfigurationImpl implements ConfigurationSPI {
    private final static Logger log = LoggerFactory.getLogger(ConfigurationImpl.class);
    private final static Pattern FILTER_XML = Pattern.compile("_config.filters.(\\w+).xml");
    private final Map<String, EnhancerPlugin> enhancerPlugins;
    final List<RuleGroup> rules = new ArrayList<RuleGroup>();
    final Set<String> includedFiles = new HashSet<String>();
    final Stack<File> includeStack = new Stack<File>();
    private ConfigStackElement currentStack;
    String storeTransformedClassesPath;
    private boolean verifyClasses;
    private Map<String, ParameterInfo> paramInfo = new HashMap<String, ParameterInfo>();
    private Set<String> enhancers = new HashSet<String>();
    private Map<String, List<DefaultMethodImplInfo>> defaultMethods = new HashMap<String, List<DefaultMethodImplInfo>>();
    private EnhancementRegistry enhancements = new EnhancementRegistryImpl();
    private long maxAge = 8 * 24 * 3600 * 1000;
    private long maxSize = 2L * 1024 * 1024 * 1024;
    private final String configFile;
    private NetworkExportParams networkExportParams;
    private Map<String, ProfilerProperty> properties = new HashMap<>();

    private final List<MetricsConfiguration> metrics = new ArrayList<>();
    private final List<MetricsDescription> systemMetrics = new ArrayList<>();

    {
        getParameterInfo("common.started");
        getParameterInfo("java.thread");
    }

    public ConfigurationImpl(String configFile) throws ParserConfigurationException, IOException, SAXException {
        //noinspection unchecked
        this.enhancerPlugins = (Map) Bootstrap.getPlugin(EnhancerRegistryPlugin.class).getEnhancersMap();
        this.configFile = configFile;
        parseFile(configFile);
        String verifyClassesProp = PropertyFacadeBoot.getProperty(Profiler.class.getName() + ".verify-classes", null);
        if (verifyClassesProp != null) {
            this.verifyClasses = Boolean.parseBoolean(verifyClassesProp);
            log.debug("Overriding verify classes property with {}", verifyClasses);
        }
        includedFiles.clear();
        includeStack.clear();
        Bootstrap.registerPlugin(Configuration.class, this);
    }

    private void parseFile(String configFile) throws ParserConfigurationException, IOException, SAXException {
        parseFile(new File(configFile));
    }

    private void parseFile(File file) throws ParserConfigurationException, IOException, SAXException {
        log.debug("Loading configuration from {}", file.getAbsolutePath());
        final String fileName = file.getName();
        String overrideFileName;
        if ("super".equalsIgnoreCase(fileName)) {
            final File lastIncluded = includeStack.peek();
            overrideFileName = lastIncluded.getName();
            if (!overrideFileName.contains(".override")) {
                log.warn("Unable to include 'super' file for {} since its name does not contain '.override'", lastIncluded.getAbsolutePath());
                return;
            }
            overrideFileName = overrideFileName.replace(".override", "");
        } else {
            int lastDot = fileName.lastIndexOf('.');
            if (lastDot >= 0)
                overrideFileName = fileName.substring(0, lastDot) + ".override" + fileName.substring(lastDot);
            else
                overrideFileName = fileName + ".override";
        }
        File realFile = new File(file.getParentFile(), overrideFileName);
        if (realFile.exists()) file = realFile;
        else if (!includeStack.isEmpty()) {
            // Allow override to be placed in the config root
            File realFile2 = new File(includeStack.get(0).getParentFile(), overrideFileName);
            if (realFile2.exists()) {
                log.debug("Found override for {} in {}. Please put into {}",
                        new Object[]{file.getAbsolutePath(), realFile2.getAbsolutePath(), realFile.getAbsolutePath()});
                file = realFile2;
            }
        }
        Matcher m = FILTER_XML.matcher(file.getName());
        if (m.matches() && !includeStack.isEmpty()) {
            String dir = m.group(1);
            File realFile2 = new File(includeStack.get(0).getParentFile(), dir);
            if (realFile2.exists()) {
                log.debug("Found override for {} in {}. Please put into {}",
                        new Object[]{file.getAbsolutePath(), realFile2.getAbsolutePath(), realFile.getAbsolutePath()});
                file = realFile2;
            }
        }

        if (!includedFiles.add(file.getAbsolutePath())) return;
        log.debug("Parsing {} {}", file.isDirectory() ? "directory" : "file", file.getAbsolutePath());

        includeStack.push(file);
        try {
            if (file.isDirectory()) {
                parseDirectory(file);
                return;
            }
            final DocumentBuilderFactory bf = DocumentBuilderFactory.newInstance();
            final DocumentBuilder db = bf.newDocumentBuilder();
            final Document doc = db.parse(file);

            parseFile(doc.getDocumentElement());
        } finally {
            includeStack.pop();
        }
    }

    private void parseDirectory(File file) throws IOException, SAXException, ParserConfigurationException {
        File[] files = file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.isDirectory() || pathname.getName().endsWith(".xml");
            }
        });
        if (files == null || files.length == 0) {
            log.debug("Directory {} is empty", file);
            return;
        }
        Arrays.sort(files, new Comparator<File>() {
            public int compare(File o1, File o2) {
                return o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase());
            }
        });
        for (File configFile : files) {
            parseFile(configFile);
        }
    }

    private void parseFile(Element doc) throws IOException, SAXException, ParserConfigurationException {
        int tagPosition = 1;
        String tagName = null;
        ConfigStackElement parentStack = currentStack;
        try {
            if (!tagIsRequired(doc)) {
                log.debug("Ignoring file " + includeStack.peek().getAbsolutePath() + " since condition did not match");
                return;
            }

            Map<String, Integer> tagsCount = new HashMap<String, Integer>();
            for (Node node = doc.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                Element e = (Element) node;
                tagName = e.getTagName().toLowerCase();

                Integer pos = tagsCount.get(tagName);
                if (pos == null)
                    tagsCount.put(tagName, pos = 1);
                tagPosition = pos;

                if (!tagIsRequired(e)) {
                    log.debug("Ignoring tag #" + tagPosition+ " (" + tagName +  ") in file " + includeStack.peek().getAbsolutePath() + " since condition did not match");
                    continue;
                }
                currentStack = new ConfigStackElement(tagName, tagPosition, includeStack.peek().getName(), parentStack);
                if ("ruleset".equals(tagName))
                    parseRuleset(e);
                else if ("enhancer".equals(tagName))
                    parseEnhancer(e);
                else if ("parameters".equals(tagName))
                    parseParameters(e);
                else if ("log-retention".equals(tagName))
                    parseRetention(e);
                else if ("verify-classes".equals(tagName))
                    parseVerifyClasses(e);
                else if ("store-transformed-classes".equals(tagName)) {
                    String storePath = XMLHelper.getTextContent(node);
                    if (storePath.length() == 0)
                        storePath = DumpRootResolver.dumpRoot + "/classes";
                    storeTransformedClassesPath = storePath;
                } else if ("add-default-implementation".equals(tagName)) {
                    parseAddDefaultImplementation(e);
                } else if ("metrics-collection".equals(tagName)) {
                    parseMetricsCollection(e);
                } else if ("call-export".equals(tagName)) {
                    parseCallExport(e);
                } else if ("properties".equals(tagName)) {
                    parseProperties(e);
                }
            }
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unable to parse tag #" + tagPosition + " (" + tagName + ") in file " + includeStack.peek().getAbsolutePath(), t);
        } finally {
            currentStack = parentStack;
        }
    }

    private void parseCallExport(Element callExportXml) {
        NetworkExportParams exportParams = new NetworkExportParamsImpl();
        exportParams.setIncludedParams(new LinkedList<String>());
        exportParams.setExcludedParams(new LinkedList<String>());
        exportParams.setSystemProperties(new LinkedList<String>());

        ConfigStackElement parentStack = currentStack;

        try {
            int position = 0;
            for (Node node = callExportXml.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                position++;
                Element e = (Element) node;

                String tagName = e.getTagName().toLowerCase();
                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);

                try {
                    if ("host".equals(tagName)) {
                        exportParams.setHost(XMLHelper.getTextContent(e));
                    } else if ("port".equals(tagName)) {
                        try {
                            exportParams.setPort(Integer.parseInt(XMLHelper.getTextContent(e)));
                        } catch (NumberFormatException e1) {
                            /**/
                        }
                    } else if ("socket-timeout".equals(tagName)) {
                        try {
                            exportParams.setSocketTimeout(Integer.parseInt(XMLHelper.getTextContent(e)));
                        } catch (NumberFormatException e1) {
                            /**/
                        }
                    } else if ("include-params".equals(tagName)) {
                        parseCallExportIncludeParams(e, exportParams);
                    } else if ("exclude-params".equals(tagName)) {
                        parseCallExportExcludeParams(e, exportParams);
                    } else if ("system-properties".equals(tagName)) {
                        parseCallExportSystemProperties(e, exportParams);
                    } else if ("filters".equals(tagName)) {
                        parseCallFilter(e, (FilterOperatorLogical) exportParams.getFilter());
                    }
                }
                catch (Throwable t) {
                    StringWriter buffer = new StringWriter(200);
                    buffer.write("Unable to parse callExport");
                    currentStack.toString(buffer.getBuffer());
                    buffer.write(" in ");
                    try {
                        TransformerFactory transFactory = TransformerFactory.newInstance();
                        Transformer transformer = transFactory.newTransformer();
                        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                        transformer.transform(new DOMSource(callExportXml), new StreamResult(buffer));
                    }
                    catch (Throwable impossible) {
                        // ignore
                    }
                    String text = buffer.toString();
                    int lastWs = text.length() - 1;
                    while (lastWs > 0 && Character.isWhitespace(text.charAt(lastWs)))
                        lastWs--;
                    if (lastWs > 0)
                        text = text.substring(0, lastWs + 1);

                    throw new IllegalArgumentException(text, t);
                }
            }
        }
        finally {
            currentStack = parentStack;
        }

        if(StringUtils.isEmpty(exportParams.getHost())) {
            log.error("Host is required in {}", currentStack);
            return;
        } else if(exportParams.getPort() == 0) {
            log.error("Port is required in {}", currentStack);
            return;
        }
        this.networkExportParams = exportParams;
    }

    private void parseCallExportIncludeParams(Element e, NetworkExportParams exportParams) {
        ConfigStackElement parentStack = currentStack;
        final NodeList includeParams = e.getElementsByTagName("include-param");
        for (int i = 0; i < includeParams.getLength(); i++) {
            final Node includeParam = includeParams.item(i);
            try {
                if (includeParam instanceof Element) {

                    currentStack = new ConfigStackElement("include-params", i + 1, includeStack.peek().getName(), parentStack);
                    String content = XMLHelper.getTextContent(includeParam);
                    if (StringUtils.isNotEmpty(content)) {
                        exportParams.getIncludedParams().add(content);
                    } else {
                        log.error("Ignoring tag {} since condition did not match", currentStack);
                    }
                }
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse include-params " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }
    }

    private void parseCallExportExcludeParams(Element e, NetworkExportParams exportParams) {
        ConfigStackElement parentStack = currentStack;
        final NodeList excludeParams = e.getElementsByTagName("exclude-param");
        for (int i = 0; i < excludeParams.getLength(); i++) {
            final Node excludeParam = excludeParams.item(i);
            try {
                if (excludeParam instanceof Element) {

                    currentStack = new ConfigStackElement("exclude-params", i + 1, includeStack.peek().getName(), parentStack);
                    String content = XMLHelper.getTextContent(excludeParam);
                    if (StringUtils.isNotEmpty(content)) {
                        exportParams.getExcludedParams().add(content);
                    } else {
                        log.error("Ignoring tag {} since condition did not match", currentStack);
                    }
                }
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse exclude-params " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }
    }

    private void parseCallExportSystemProperties(Element e, NetworkExportParams exportParams) {
        ConfigStackElement parentStack = currentStack;
        final NodeList properties = e.getElementsByTagName("property");
        for (int i = 0; i < properties.getLength(); i++) {
            final Node property = properties.item(i);
            try {
                if (property instanceof Element) {

                    currentStack = new ConfigStackElement("system-properties", i + 1, includeStack.peek().getName(), parentStack);
                    String content = XMLHelper.getTextContent(property);
                    if (StringUtils.isNotEmpty(content)) {
                        exportParams.getSystemProperties().add(content);
                    } else {
                        log.error("Ignoring tag {} since condition did not match", currentStack);
                    }
                }
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse system-properties " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }
    }

    private void parseProperties(Element propertiesXml) {
        ConfigStackElement parentStack = currentStack;

        try {
            int position = 0;
            for (Node node = propertiesXml.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                position++;
                Element e = (Element) node;

                String tagName = e.getTagName().toLowerCase();
                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);

                try {
                    if ("property".equals(tagName)) {
                        String key = e.getAttribute("name");
                        if (StringUtils.isEmpty(key)) {
                            log.error("{} in {}", "Attribute key is required", currentStack.toString());
                            return;
                        }
                        ProfilerProperty property = properties.get(key);
                        if(property == null) {
                            property = new ProfilerPropertyImpl();
                        }
                        String curValue = e.getAttribute("value");
                        if (StringUtils.isEmpty(curValue)) {
                            property.addValues(parsePropertyValues(e));
                        } else {
                            property.addValue(curValue);
                        }
                        properties.put(key, property);
                    }
                }
                catch (Throwable t) {
                    StringWriter buffer = new StringWriter(200);
                    buffer.write("Unable to parse properties");
                    currentStack.toString(buffer.getBuffer());
                    buffer.write(" in ");
                    try {
                        TransformerFactory transFactory = TransformerFactory.newInstance();
                        Transformer transformer = transFactory.newTransformer();
                        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                        transformer.transform(new DOMSource(propertiesXml), new StreamResult(buffer));
                    }
                    catch (Throwable impossible) {
                        // ignore
                    }
                    String text = buffer.toString();
                    int lastWs = text.length() - 1;
                    while (lastWs > 0 && Character.isWhitespace(text.charAt(lastWs)))
                        lastWs--;
                    if (lastWs > 0)
                        text = text.substring(0, lastWs + 1);

                    throw new IllegalArgumentException(text, t);
                }
            }
        }
        finally {
            currentStack = parentStack;
        }
    }

    private List<String> parsePropertyValues(Element property) {
        ConfigStackElement parentStack = currentStack;
        List<String> result = new ArrayList<>();
        final NodeList values = property.getElementsByTagName("value");
        for (int i = 0; i < values.getLength(); i++) {
            final Node value = values.item(i);
            try {
                currentStack = new ConfigStackElement("value", i + 1, includeStack.peek().getName(), parentStack);
                String valueStr = XMLHelper.getTextContent(value);
                result.add(valueStr);
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse value " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }
        return result;
    }

    private void parseAddDefaultImplementation(Element element) {
        boolean argsOk = true;
        String className = element.getAttribute("class");
        if (className == null || className.length() == 0) {
            argsOk = false;
            log.warn("Please specify class name (in java native format like java/lang/Throwable) via class attribute for tag {}", currentStack);
        } else {
            if (className.indexOf('/') == -1 && className.indexOf('.') != -1) {
                log.debug("Converting . to / in class name {} of tag {}", className, currentStack);
                className = className.replace('.', '/');
            }
        }

        String methodName = element.getAttribute("methodName");
        if (methodName == null || methodName.length() == 0) {
            argsOk = false;
            log.warn("Please specify @methodName for tag {}", currentStack);
        }

        String methodDescr = element.getAttribute("methodDescr");
        if (methodDescr == null || methodDescr.length() == 0) {
            argsOk = false;
            log.warn("Please specify @methodDescr descriptor (in java native format like getCause()V ) for tag {}", currentStack);
        }

        String access = element.getAttribute("access");
        int accessInt;

        if (access == null || access.length() == 0)
            accessInt = Opcodes.ACC_PUBLIC;
        else {
            try {
                accessInt = Integer.parseInt(access);
            } catch (NumberFormatException nfe) {
                log.warn("Unable to parse access attribute {} for tag {}. Will use just 'public'", access, currentStack);
                accessInt = Opcodes.ACC_PUBLIC;
            }
        }

        if (!argsOk)
            return;

        List<DefaultMethodImplInfo> methods = defaultMethods.get(className);
        if (methods == null)
            defaultMethods.put(className, methods = new ArrayList<DefaultMethodImplInfo>());
        String ifEnhancer = element.getAttribute("if-enhancer");
        if (ifEnhancer.length() == 0) ifEnhancer = null;
        String skipSuper = element.getAttribute("skip-super");
        if (skipSuper.length() == 0) skipSuper = "false";
        methods.add(new DefaultMethodImplInfo(className, methodName, methodDescr, ifEnhancer, accessInt, Boolean.valueOf(skipSuper)));
    }

    private boolean tagIsRequired(Element tag) {
        if (!tag.hasAttributes()) return true;

        String property;

        property = tag.getAttribute("if");

        if (property != null && property.length() > 0) { // true for positive values, false for others and nulls
            String value = System.getProperty(property);
            boolean result = "true".equalsIgnoreCase(value) || "yes".equalsIgnoreCase(value) ||
                    "y".equalsIgnoreCase(value) || "1".equals(value);
            log.debug("Resulting value for 'if' of property {} with value {} is {}", new Object[]{property, value, result});
            return result;
        }

        property = tag.getAttribute("unless");

        if (property != null && property.length() > 0) { // true for negative and null values, false otherwise
            String value = System.getProperty(property);
            boolean result = value == null || "false".equalsIgnoreCase(value) || "no".equalsIgnoreCase(value) ||
                    "n".equalsIgnoreCase(value) || "0".equals(value);
            log.debug("Resulting value for 'unless' of property {} with value {} is {}", new Object[]{property, value, result});
            return result;
        }

        property = tag.getAttribute("if-exists");
        if (property != null && property.length() > 0) {
            boolean result = System.getProperty(property) != null;
            log.debug("Resulting value for 'if-exists' of property {} is {}", property, result);
            return result;
        }

        property = tag.getAttribute("unless-exists");
        if (property != null && property.length() > 0) {
            boolean result = System.getProperty(property) == null;
            log.debug("Resulting value for 'unles-exists' of property {} is {}", property, result);
            return result;
        }

        return true;
    }

    private void parseEnhancer(Element element) {
        final String enhancerName = XMLHelper.getTextContent(element);
        if (enhancerName.length() == 0) {
            log.warn("Please specify enhancer name in file {}", includeStack.peek().getAbsolutePath());
            return;
        }

        if (!enhancers.add(enhancerName)) {
            log.debug("Skipping second loading of enhancer {}, the second appearance is in file {}", enhancerName, includeStack.peek().getAbsolutePath());
            return;
        }

        EnhancerPlugin plugin = enhancerPlugins.get(enhancerName);
        if (plugin == null) {
            log.warn("Unable to find enhancer {}", enhancerName);
            return;
        }
        plugin.init(element, this);
        plugin.setStackTraceAtCreate(currentStack);
        getEnhancementRegistry().addFilter(enhancerName, plugin);
    }

    private void parseRuleset(Element ruleset) throws IOException, SAXException, ParserConfigurationException {
        ConfigStackElement parentStack = currentStack;
        String ifEnhancer = ruleset.getAttribute("if-enhancer");
        if (ifEnhancer.length() == 0) ifEnhancer = null;
        final NodeList rules = ruleset.getElementsByTagName("rule");
        for (int i = 0; i < rules.getLength(); i++) {
            final Node rule = rules.item(i);
            try {
                if (rule instanceof Element) {
                    if (!tagIsRequired((Element) rule)) {
                        log.debug("Ignoring ruleset tag #" + i + " in file " + includeStack.peek().getAbsolutePath() + " since condition did not match");
                        continue;
                    }
                    currentStack = new ConfigStackElement("rule", i + 1, includeStack.peek().getName(), parentStack);
                    parseRule((Element) rule, ifEnhancer);
                }
            } catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse rule " + (i + 1), t);
            } finally {
                currentStack = parentStack;
            }
        }
    }

    private void parseRule(Element ruleXml, String rulesetIfEnhancer) throws IOException, SAXException, ParserConfigurationException {
        final String src = ruleXml.getAttribute("src");
        if (src.length() > 0) {
            parseFile(new File(includeStack.peek().getParent(), src));
            return;
        }

        Rule rule = new Rule();
        rule.setStackTraceAtCreate(currentStack);
        String ifEnhancer = ruleXml.getAttribute("if-enhancer");
        if (ifEnhancer.length() == 0) ifEnhancer = rulesetIfEnhancer;
        rule.setIfEnhancer(ifEnhancer);
        ConfigStackElement parentStack = currentStack;
        try {
            int position = 0;
            for (Node node = ruleXml.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                position++;
                Element e = (Element) node;

                String tagName = e.getTagName().toLowerCase();
                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);
                if (!tagIsRequired(e)) {
                    log.debug("Ignoring tag {} since condition did not match", currentStack);
                    continue;
                }

                String content = XMLHelper.getTextContent(e);
                try {
                    if (content == null) content = "";
                    if ("class".equals(tagName)) {
                        rule.addClass(content);
                    } else if ("extends".equals(tagName) || "implements".equals(tagName)) {
                        rule.addSuperclass(content);
                    } else if ("method".equals(tagName)) {
                        rule.addIncludedMethod(content);
                    } else if ("include-method".equals(tagName)) {
                        rule.addIncludedMethod(content);
                    } else if ("exclude-method".equals(tagName)) {
                        rule.addExcludedMethod(content);
                    } else if ("method-modifier".equals(tagName)) {
                        rule.methodModifier(content);
                    } else if ("do-not-profile".equals(tagName)) {
                        rule.doNotProfile();
                    } else if ("minimum-method-size".equals(tagName)) {
                        try {
                            rule.setMinimumMethodSize(Integer.parseInt(content));
                        } catch (NumberFormatException e1) {
                            /**/
                        }
                    } else if ("minimum-method-lines".equals(tagName)) {
                        try {
                            rule.setMinimumMethodLines(Integer.parseInt(content));
                        } catch (NumberFormatException e1) {
                            /**/
                        }
                    } else if ("minimum-method-loops".equals(tagName)) {
                        try {
                            rule.setMinimumMethodBackJumps(Integer.parseInt(content));
                        } catch (NumberFormatException e1) {
                            /**/
                        }
                    } else if ("class-modifier".equals(tagName)) {
                        rule.classModifier(content);
                    } else {
                        MethodInstrumenter mi;
                        mi = parseMethodAcceptor(e, tagName, content);
                        if (mi == null) continue;
                        mi.init(e, this);

                        rule.addMethodEditor(mi);
                    }
                } catch (Throwable t) {
                    StringWriter buffer = new StringWriter(200);
                    buffer.write("Unable to parse rule ");
                    currentStack.toString(buffer.getBuffer());
                    buffer.write(" in ");
                    try {
                        TransformerFactory transFactory = TransformerFactory.newInstance();
                        Transformer transformer = transFactory.newTransformer();
                        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                        transformer.transform(new DOMSource(ruleXml), new StreamResult(buffer));
                    } catch (Throwable impossible) {
                        /* ignore */
                    }
                    String text = buffer.toString();
                    int lastWs = text.length() - 1;
                    while (lastWs > 0 && Character.isWhitespace(text.charAt(lastWs)))
                        lastWs--;
                    if (lastWs > 0)
                        text = text.substring(0, lastWs + 1);
                    throw new IllegalArgumentException(text, t);
                }
            }
        } finally {
            currentStack = parentStack;
        }
        if (!rules.isEmpty()) {
            RuleGroup group = rules.get(rules.size() - 1);
            if (group.add(rule))
                return;
            if (group instanceof RuleListGroup && rules.size() > 1 && !rule.getClassNames().isEmpty()) {
                // If adding hashable rule, try to (..., RuleHashGroup, RuleListGroup) list of groups, try to add the rule
                // to hash group. This works if none of the "RuleListGroup" matches the class name in question
                boolean canMoveUp = true;
                checkClassNames:
                for (String className : rule.getClassNames()) {
                    List<Rule> rulesForClass = group.getRulesForClass(className);
                    for (Rule ruleForClass : rulesForClass) {
                        if (ruleForClass.classNameMatches(className)){
                            canMoveUp = false;
                            break checkClassNames;
                        }
                    }
                }
                if (canMoveUp) {
                    RuleGroup prev = rules.get(rules.size() - 2);
                    if (prev.add(rule))
                        return;
                }
            }
        }
        rules.add(RuleGroup.of(rule));
    }

    private MethodInstrumenter parseMethodAcceptor(Element e, String tagName, String content) {
        if (tagName.startsWith("execute-")) {
            if (tagName.equals("execute-before") && !e.hasAttribute("exception-only") && !e.hasAttribute("exception"))
                return new ExecuteMethodBefore();
            if (tagName.equals("execute-instead"))
                return new ExecuteMethodInstead();
            return new GuardedAction(new ExecuteMethodAfter().init(e, this));
        }
        if (tagName.startsWith("log-parameter")) {
            if (!parseParamType(e)) return null;
            MethodInstrumenter res = new LogParameter();
            if (!tagName.endsWith("-when"))
                return res;
            return new GuardedAction(res.init(e, this));
        }
        if (tagName.startsWith("process-argument")) {
            String argument = e.getAttribute("argument");
            if (argument.length() == 0) {
                log.warn("Please, specify 'argument' attribute for tag {}", currentStack);
                return null;
            }
            e.setAttribute("store-result-to-argument", argument);
            return new ExecuteMethodBefore();
        }
        if (tagName.startsWith("log-return")) {
            if (!parseParamType(e)) return null;
            MethodInstrumenter res = new LogReturn();
            if (!tagName.endsWith("-when"))
                return res;
            return new GuardedAction(res.init(e, this));
        }
        if (tagName.startsWith("log-this")) {
            if (!parseParamType(e)) return null;
            MethodInstrumenter res = new LogThis();
            if (!tagName.endsWith("-when"))
                return res;
            return new GuardedAction(res.init(e, this));
        }
        if (tagName.startsWith("log-exception")) {
            return new LogException();
        }
        if ("method-editor".equals(tagName)) {
            if (content.length() == 0) {
                log.error("log-after requires class name to be specified in content (without com.netcracker.profiler.instrument.custom.impl package)");
                return null;
            }

            try {
                final Class<?> aClass = Class.forName("com.netcracker.profiler.instrument.custom.impl." + content);
                return (MethodInstrumenter) aClass.newInstance();
            } catch (ClassNotFoundException ex) {
                log.error("Unable to load custom implementation {}", content, ex);
            } catch (InstantiationException ex) {
                log.error("Unable to instantiate {}", content, ex);
            } catch (IllegalAccessException ex) {
                log.error("Unable to instantiate {}", content, ex);
            } catch (ClassCastException ex) {
                log.error("Unable to instantiate {}", content, ex);
            }
        }
        if ("when".equals(tagName))
            return new GuardedAction(parseActions(e));
        return null;
    }

    private MethodAcceptor parseActions(Element root) {
        ConfigStackElement parentStack = currentStack;
        MethodInstrumenter result = null;
        MethodAcceptorsList list = null;
        try {
            int position = 0;
            for (Node node = root.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                position++;
                Element e = (Element) node;
                String tagName = e.getTagName().toLowerCase();
                String content = XMLHelper.getTextContent(e);

                if (content == null) content = "";
                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);
                MethodInstrumenter mi = parseMethodAcceptor(e, tagName, content);
                if (mi == null) continue;

                mi.init(e, this);

                if (result == null)
                    result = mi;
                else if (list != null)
                    list.add(mi);
                else {
                    list = new MethodAcceptorsList();
                    list.add(result);
                    list.add(mi);
                }
            }
        } finally {
            currentStack = parentStack;
        }
        return list == null ? result : list;
    }

    private void parseRetention(Element e) {
        String maxAge = e.getAttribute("max-age");
        if (maxAge.length() > 0)
            this.maxAge = DurationParser.parseDuration(maxAge, this.maxAge);

        String maxSize = e.getAttribute("max-size");
        if (maxSize.length() > 0)
            this.maxSize = SizeParser.parseSize(maxSize, this.maxSize);

        log.debug("Dumper will erase profiler logs when they are older than {} days or they consume more than {} mb", this.maxAge / 1000.0 / 3600 / 24, this.maxSize / 1024 / 1024);
    }

    private void parseVerifyClasses(Element e) {
        String content = XMLHelper.getTextContent(e);
        verifyClasses = content == null || content.trim().length() == 0 || Boolean.valueOf(content);
        if (verifyClasses) {
            log.debug("Class verification is enabled");
        }
    }

    public boolean isVerifyClassEnabled() {
        return verifyClasses;
    }

    private void parseParameters(Element params) {
        for (Node node = params.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (!(node instanceof Element)) continue;
            Element e = (Element) node;
            String tagName = e.getTagName().toLowerCase();
            if (!"parameter".equals(tagName)) continue;
            parseParamType(e);
        }
    }

    private boolean parseParamType(Element e) {
        String eventName = e.getAttribute("name");
        if (eventName.length() == 0) {
            log.error("Please, specify name attribute for tag {}", currentStack);
            return false;
        }

        final ParameterInfo info = getParameterInfo(eventName);
        info.parse(e);

        return true;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setParamType(String eventName, int paramType) {
        ParameterInfo info = getParameterInfo(eventName);
        if (paramType != -1)
            info.paramType(paramType);
    }


    public Map<String, ParameterInfo> getParametersInfo() {
        return paramInfo;
    }

    public ParameterInfo getParameterInfo(String name) {
        ParameterInfo info = paramInfo.get(name);
        if (info == null) {
            info = new ParameterInfo(name);
            paramInfo.put(name, info);
        }
        return info;
    }

    public Collection<Rule> getRulesForClass(String className, Collection<Rule> rules) {
        if (className == null)
            return Collections.emptyList();
        if (rules != null)
            rules.clear();
        List<RuleGroup> groups = this.rules;
        findRules:
        for (int i = 0; i < groups.size(); i++) {
            RuleGroup group = groups.get(i);
            List<Rule> ruleList = group.getRulesForClass(className);
            if (ruleList == null) continue;
            boolean needRecheck = group instanceof RuleListGroup;
            for (int j = 0; j < ruleList.size(); j++) {
                Rule rule = ruleList.get(j);
                if (needRecheck && !rule.classNameMatches(className))
                    continue;
                boolean allMethodsMatch = rule.allMethodsMatch() && !rule.hasSuperclassCriteria();
                if (!allMethodsMatch || !rule.doesNotChangeClass()) {
                    if (rules == null)
                        rules = new ArrayList<Rule>();
                    rules.add(rule);
                }
                if (allMethodsMatch && rule.getIfEnhancer() == null)
                    break findRules;
            }
        }
        if (rules == null)
            return Collections.emptyList();
        /* In case all the rules do not change the class we just clear the collection.*/
        for (Rule rule : rules)
            if (!rule.doesNotChangeClass())
                return rules;
        rules.clear();
        return Collections.emptyList();
    }

    private void parseMetricsCollection(Element metricsCollection) {
        log.debug("metrics collection");
        ConfigStackElement parentStack = currentStack;
        int outputVersion = ProfilerData.METRICS_OUTPUT_VERSION;
        try {
            outputVersion = Integer.parseInt(metricsCollection.getAttribute("outputVersion"));
        } catch (NumberFormatException e1) {
            /**/
        }
        final NodeList callTypes = metricsCollection.getElementsByTagName("call-type");
        for (int i = 0; i < callTypes.getLength(); i++) {
            final Node callType = callTypes.item(i);
            try {
                if (callType instanceof Element) {
                    currentStack = new ConfigStackElement("call-type", i + 1, includeStack.peek().getName(), parentStack);
                    log.info("parse call type #{}", i + 1);
                    parseCallType((Element) callType, outputVersion);
                }
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse callType " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }

        final NodeList systemMetrics = metricsCollection.getElementsByTagName("system-metrics");
        for (int i = 0; i < systemMetrics.getLength(); i++) {
            final Node systemMetric = systemMetrics.item(i);
            try {
                if (systemMetric instanceof Element) {
                    currentStack = new ConfigStackElement("system-metrics", i + 1, includeStack.peek().getName(), parentStack);
                    log.info("parse system metric #{}", i + 1);
                    this.systemMetrics.addAll(parseMetrics((Element) systemMetric));
                }
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse callType " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }
    }

    private void parseCallType(Element callTypeXml, int outputVersion) {
        CallType callType = new CallType();

        String name = callTypeXml.getAttribute("name");
        if(StringUtils.isEmpty(name)) {
            name = callTypeXml.getAttribute("output-type");
        }
        if (StringUtils.isEmpty(name)) {
            log.error("{} in {}", "Attribute name is required", currentStack.toString());
            return;
        }
        callType.getMetricsConfigurationImpl().setName(name);
        String isCustom = callTypeXml.getAttribute("is-custom");
        callType.getMetricsConfigurationImpl().setIsCustom(isCustom);

        try {
            outputVersion = Integer.parseInt(callTypeXml.getAttribute("outputVersion"));
        } catch (NumberFormatException e1) {
            /**/
        }
        callType.getMetricsConfigurationImpl().setOutputVersion(outputVersion);

        ConfigStackElement parentStack = currentStack;

        try {
            int position = 0;
            for (Node node = callTypeXml.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                position++;
                Element e = (Element) node;

                String tagName = e.getTagName().toLowerCase();
                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);

                try {
                    if ("matching".equals(tagName)) {
                        parseCallTypeMatching(e, callType);
                    } else if ("aggregation-params".equals(tagName)) {
                        parseCallTypeAggregationParameters(e, callType);
                    } else if ("metrics".equals(tagName)) {
                        callType.getMetricsConfigurationImpl().getMetrics().addAll(parseMetrics(e));
                    } else if ("filters".equals(tagName)) {
                        parseCallFilter(e, (FilterOperatorLogical) callType.getMetricsConfigurationImpl().getFilter());
                    }
                }
                catch (Throwable t) {
                    StringWriter buffer = new StringWriter(200);
                    buffer.write("Unable to parse callType");
                    currentStack.toString(buffer.getBuffer());
                    buffer.write(" in ");
                    try {
                        TransformerFactory transFactory = TransformerFactory.newInstance();
                        Transformer transformer = transFactory.newTransformer();
                        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                        transformer.transform(new DOMSource(callTypeXml), new StreamResult(buffer));
                    }
                    catch (Throwable impossible) {
                        // ignore
                    }
                    String text = buffer.toString();
                    int lastWs = text.length() - 1;
                    while (lastWs > 0 && Character.isWhitespace(text.charAt(lastWs)))
                        lastWs--;
                    if (lastWs > 0)
                        text = text.substring(0, lastWs + 1);

                    throw new IllegalArgumentException(text, t);
                }
            }
        }
        finally {
            currentStack = parentStack;
        }
        if (callType.getMetricsConfigurationImpl().getMetrics().size() != 0 && callType.getMetricsConfigurationImpl().getMatchingClass() != null)
            metrics.add(callType.getMetricsConfigurationImpl());
        else if (callType.getMetricsConfigurationImpl().getMetrics().size() == 0)
            log.error("{} in {}", "Metrics is required", currentStack.toString());
        else if (callType.getMetricsConfigurationImpl().getMatchingClass() == null)
            log.error("{} in {}", "Matching class is required", currentStack.toString());

    }

    private void parseCallTypeMatching(Element matching, CallType callType) {
        if (StringUtils.isNotEmpty((matching).getAttribute("class")))
            callType.getMetricsConfigurationImpl().setMatchingClass((matching).getAttribute("class"));
        if (StringUtils.isNotEmpty((matching).getAttribute("method")))
            callType.getMetricsConfigurationImpl().setMatchingMethod((matching).getAttribute("method"));
    }

    private List<MetricsDescription> parseMetrics(Element metrics) {
        ConfigStackElement parentStack = currentStack;
        try {
            int position = 0;
            List<MetricsDescription> metricsDescriptions = new ArrayList<>();
            for (Node node = metrics.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) continue;
                position++;
                Element e = (Element) node;

                String tagName = e.getTagName().toLowerCase();

                if("counts".equals(tagName)) { //For support of old configuration format
                    tagName = "count";
                } else if("durations".equals(tagName)) {
                    tagName = "duration";
                }

                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);

                NamedNodeMap attributesMap = e.getAttributes();
                MetricsDescription md = new MetricsDescription(tagName);
                metricsDescriptions.add(md);
                //callType.getMetricsConfigurationImpl().getMetrics().add(md);
                for (int i = 0; i < attributesMap.getLength(); ++i) {
                    Node attr = attributesMap.item(i);
                    md.setParameter(attr.getNodeName(), attr.getNodeValue());
                }
            }
            return metricsDescriptions;
        }
        finally {
            currentStack = parentStack;
        }
    }


    private void parseCallTypeAggregationParameters(Element params, CallType callType) {
        ConfigStackElement parentStack = currentStack;
        final NodeList inputParams = params.getElementsByTagName("input-param");
        for (int i = 0; i < inputParams.getLength(); i++) {
            final Node inputParam = inputParams.item(i);
            try {
                if (inputParam instanceof Element) {

                    currentStack = new ConfigStackElement("input-params", i + 1, includeStack.peek().getName(), parentStack);
                    String parameterName = ((Element) inputParam).getAttribute("name");
                    if (StringUtils.isNotEmpty(parameterName)) {
                        String parameterDisplayName = ((Element) inputParam).getAttribute("display-name");
                        if (StringUtils.isEmpty(parameterDisplayName)) {
                            callType.getMetricsConfigurationImpl().getAggregationParameters().add(new AggregationParameterDescriptor(parameterName));
                        } else {
                            callType.getMetricsConfigurationImpl().getAggregationParameters().add(new AggregationParameterDescriptor(parameterName, parameterDisplayName));
                        }
                    }
                    else {
                        log.error("Ignoring tag {} since condition did not match", currentStack);
                    }
                }
            }
            catch (Throwable t) {
                if (t instanceof IllegalArgumentException && i == 0)
                    throw (IllegalArgumentException) t;
                else
                    throw new IllegalArgumentException("Unable to parse input-params " + (i + 1), t);
            }
            finally {
                currentStack = parentStack;
            }
        }
    }

    private void parseCallFilter(Element operator, FilterOperatorLogical filterElement) {
        ConfigStackElement parentStack = currentStack;
        try {
            int position = 0;
            for (Node node = operator.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (!(node instanceof Element)) {
                    continue;
                }
                position++;
                Element e = (Element) node;

                String tagName = e.getTagName().toLowerCase();
                currentStack = new ConfigStackElement(tagName, position, includeStack.peek().getName(), parentStack);

                try {
                    if ("and".equals(tagName)) {
                        FilterOperatorLogical child = new FilterOperatorAnd();
                        filterElement.addChild(child);
                        parseCallFilter(e, child);
                    } else if ("or".equals(tagName)) {
                        FilterOperatorLogical child = new FilterOperatorOr();
                        filterElement.addChild(child);
                        parseCallFilter(e, child);
                    } else if ("not".equals(tagName)) {
                        FilterOperatorLogical child = new FilterOperatorNot();
                        filterElement.addChild(child);
                        parseCallFilter(e, child);
                    } else {
                        parseCallFilterCondition(e, filterElement);
                    }
                }
                catch (Throwable t) {
                    StringWriter buffer = new StringWriter(200);
                    buffer.write("Unable to parse callType ");
                    currentStack.toString(buffer.getBuffer());
                    buffer.write(" in ");
                    try {
                        TransformerFactory transFactory = TransformerFactory.newInstance();
                        Transformer transformer = transFactory.newTransformer();
                        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                        transformer.transform(new DOMSource(operator), new StreamResult(buffer));
                    }
                    catch (Throwable impossible) {
                        // ignore
                    }
                    String text = buffer.toString();
                    int lastWs = text.length() - 1;
                    while (lastWs > 0 && Character.isWhitespace(text.charAt(lastWs)))
                        lastWs--;
                    if (lastWs > 0)
                        text = text.substring(0, lastWs + 1);
                    throw new IllegalArgumentException(text, t);
                }
            }
        }
        finally {
            currentStack = parentStack;
        }

    }

    private void parseCallFilterCondition(Element e, FilterOperatorLogical filterElement) {
        ConfigStackElement parentStack = currentStack;
        try {
            String tagName = e.getTagName().toLowerCase();
            currentStack = new ConfigStackElement(tagName, 0, includeStack.peek().getName(), parentStack);

            if ("input-param".equals(tagName)) {
                parseCallParamsFilterCondition(e, filterElement);
            } else if("matching".equals(tagName)) {
                parseCallMatchingFilterCondition(e, filterElement);
            } else {
                parseCallMetricsFilterCondition(e, filterElement);
            }

        }
        finally {
            currentStack = parentStack;
        }
    }

    private void parseCallParamsFilterCondition(Element e, FilterOperatorLogical filterElement) {
        String name = e.getAttribute("name");
        String exact = e.getAttribute("exact");
        String startsWith = e.getAttribute("startsWith");
        String contains = e.getAttribute("contains");
        String endsWith = e.getAttribute("endsWith");
        boolean noConditions = true;

        if (StringUtils.isEmpty(name)) {
            log.error("Ignoring tag {} since condition did not match", currentStack);
            return;
        }

        if (StringUtils.isNotEmpty(exact)) {
            filterElement.addChild(new FilterOperatorExact(name, exact));
            noConditions = false;
        }
        if (StringUtils.isNotEmpty(startsWith)) {
            filterElement.addChild(new FilterOperatorStartsWith(name, startsWith));
            noConditions = false;
        }
        if (StringUtils.isNotEmpty(contains)) {
            filterElement.addChild(new FilterOperatorContains(name, contains));
            noConditions = false;
        }
        if (StringUtils.isNotEmpty(endsWith)) {
            filterElement.addChild(new FilterOperatorEndsWith(name, endsWith));
            noConditions = false;
        }
        if (noConditions) {
            log.error("Ignoring tag {} since condition did not match", currentStack);
        }
    }

    private void parseCallMetricsFilterCondition(Element e, FilterOperatorLogical filterElement) {
        String tagName = e.getTagName().toLowerCase();
        FilterOperatorMath metricsFilter;

        switch(tagName) {
            case "cpu":
               metricsFilter = new FilterOperatorCpu();
               break;
            case "disk-io":
                metricsFilter = new FilterOperatorDiskIO();
                break;
            case "duration":
                metricsFilter = new FilterOperatorDuration();
                break;
            case "memory":
                metricsFilter = new FilterOperatorMemory();
                break;
            case "network-io":
                metricsFilter = new FilterOperatorNetworkIO();
                break;
            case "queue-wait-time":
                metricsFilter = new FilterOperatorQueueWaitTime();
                break;
            case "transactions":
                metricsFilter = new FilterOperatorTransactions();
                break;
            default:
                return;
        }

        try {
            metricsFilter.setConstraintValue(Long.parseLong(e.getAttribute("value")));
        } catch (NumberFormatException nfe) {
            log.error("Ignoring tag {} since mandatory attribute 'value' isn't specified", currentStack);
            return;
        }

        String condition = e.getAttribute("condition");
        if(StringUtils.isEmpty(condition)) {
            log.error("Ignoring tag {} since mandatory attribute 'condition' isn't specified", currentStack);
            return;
        }

        switch (condition) {
            case "=":
                metricsFilter.setCondition(EqCondition.getInstance());
                break;
            case ">":
                metricsFilter.setCondition(GtCondition.getInstance());
                break;
            case ">=":
                metricsFilter.setCondition(GtOrEqCondition.getInstance());
                break;
            case "<":
                metricsFilter.setCondition(LtCondition.getInstance());
                break;
            case "<=":
                metricsFilter.setCondition(LtOrEqCondition.getInstance());
                break;
            default:
                log.error("Ignoring tag {} since mandatory attribute 'condition' isn't set to valid value", currentStack);
                return;
        }

        filterElement.addChild(metricsFilter);
    }

    private void parseCallMatchingFilterCondition(Element e, FilterOperatorLogical filterElement) {
        String className = e.getAttribute("class");
        String methodName = e.getAttribute("method");
        if(StringUtils.isEmpty(className)) {
            log.error("Ignoring tag {} since mandatory attribute 'class' isn't specified", currentStack);
            return;
        }
        filterElement.addChild(new FilterOperatorClassMethod(className, methodName));
    }


    public String getStoreTransformedClassesPath() {
        return storeTransformedClassesPath;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Map<String, Integer> getParamTypes() {
        return Collections.emptyMap();
    }

    public EnhancementRegistry getEnhancementRegistry() {
        return enhancements;
    }

    public String getConfigFile() {
        return configFile;
    }

    public long getLogMaxAge() {
        return maxAge;
    }

    public long getLogMaxSize() {
        return maxSize;
    }

    public NetworkExportParams getNetworkExportParams() {
        return networkExportParams;
    }

    public List<DefaultMethodImplInfo> getDefaultMethods(String className) {
        List<DefaultMethodImplInfo> methods = defaultMethods.get(className);
        if (methods == null)
            return Collections.emptyList();
        return methods;
    }

    public void addEnhancerPlugin(String name, Object plugin) {

    }

    public Object getEnhancerPlugin(String name) {
        return null;
    }

    public List<MetricsConfiguration> getMetricsConfig() {
        return metrics;
    }

    public List<MetricsDescription> getSystemMetricsConfig() {
        return systemMetrics;
    }

    public Map<String, ProfilerProperty> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConfigurationImpl that = (ConfigurationImpl) o;

        if (verifyClasses != that.verifyClasses) return false;
        if (maxAge != that.maxAge) return false;
        if (maxSize != that.maxSize) return false;
        if (enhancerPlugins != null ? !enhancerPlugins.equals(that.enhancerPlugins) : that.enhancerPlugins != null)
            return false;
        if (rules != null ? !rules.equals(that.rules) : that.rules != null) return false;
        if (includedFiles != null ? !includedFiles.equals(that.includedFiles) : that.includedFiles != null)
            return false;
        if (includeStack != null ? !includeStack.equals(that.includeStack) : that.includeStack != null) return false;
        if (currentStack != null ? !currentStack.equals(that.currentStack) : that.currentStack != null) return false;
        if (storeTransformedClassesPath != null ? !storeTransformedClassesPath.equals(that.storeTransformedClassesPath) : that.storeTransformedClassesPath != null)
            return false;
        if (paramInfo != null ? !paramInfo.equals(that.paramInfo) : that.paramInfo != null) return false;
        if (enhancers != null ? !enhancers.equals(that.enhancers) : that.enhancers != null) return false;
        if (defaultMethods != null ? !defaultMethods.equals(that.defaultMethods) : that.defaultMethods != null)
            return false;
        if (enhancements != null ? !enhancements.equals(that.enhancements) : that.enhancements != null) return false;
        if (configFile != null ? !configFile.equals(that.configFile) : that.configFile != null) return false;
        if (networkExportParams != null ? !networkExportParams.equals(that.networkExportParams) : that.networkExportParams != null)
            return false;
        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
        return systemMetrics != null ? systemMetrics.equals(that.systemMetrics) : that.systemMetrics == null;
    }

    @Override
    public int hashCode() {
        int result = enhancerPlugins != null ? enhancerPlugins.hashCode() : 0;
        result = 31 * result + (rules != null ? rules.hashCode() : 0);
        result = 31 * result + (includedFiles != null ? includedFiles.hashCode() : 0);
        result = 31 * result + (includeStack != null ? includeStack.hashCode() : 0);
        result = 31 * result + (currentStack != null ? currentStack.hashCode() : 0);
        result = 31 * result + (storeTransformedClassesPath != null ? storeTransformedClassesPath.hashCode() : 0);
        result = 31 * result + (verifyClasses ? 1 : 0);
        result = 31 * result + (paramInfo != null ? paramInfo.hashCode() : 0);
        result = 31 * result + (enhancers != null ? enhancers.hashCode() : 0);
        result = 31 * result + (defaultMethods != null ? defaultMethods.hashCode() : 0);
        result = 31 * result + (enhancements != null ? enhancements.hashCode() : 0);
        result = 31 * result + (int) (maxAge ^ (maxAge >>> 32));
        result = 31 * result + (int) (maxSize ^ (maxSize >>> 32));
        result = 31 * result + (configFile != null ? configFile.hashCode() : 0);
        result = 31 * result + (networkExportParams != null ? networkExportParams.hashCode() : 0);
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        result = 31 * result + (systemMetrics != null ? systemMetrics.hashCode() : 0);
        return result;
    }
}
