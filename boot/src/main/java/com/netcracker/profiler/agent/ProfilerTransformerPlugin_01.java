package com.netcracker.profiler.agent;

import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

public interface ProfilerTransformerPlugin_01 extends ProfilerTransformerPlugin {
    /**
     * Returns status of current (last) reload operation
     *
     * @return status of current (last) reload operation
     */
    public ReloadStatus getReloadStatus();

    /**
     * Initiates reload operation.
     * This will load new configuration to the class transformer, scan all the loaded classes and reload classes when nessesary.
     *
     * @param newConfigPath the parh to the new config file. Leave empty or null to reload configuration from current location
     *                      The reloading is offloaded to a separate thread as soon as the configuration is parsed.
     * @throws IOException                  when failed to include some file to the configuration
     * @throws SAXException                 when parsing of configuration xml failed
     * @throws ParserConfigurationException when unable to create xml parser
     */
    public void reloadConfiguration(String newConfigPath) throws IOException, SAXException, ParserConfigurationException;

    /**
     * Initiates reload operation for a set of classes
     * The reloading is performed using current configuration
     *
     * @param classNames the names of classes that should be reloaded
     * @throws IOException                  when failed to include some file to the configuration
     * @throws SAXException                 when parsing of configuration xml failed
     * @throws ParserConfigurationException when unable to create xml parser
     */
    public void reloadClasses(Set<String> classNames) throws IOException, SAXException, ParserConfigurationException;
}
