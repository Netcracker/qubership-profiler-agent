package com.netcracker.profiler.agent.plugins;

import com.netcracker.profiler.agent.Configuration_06;
import com.netcracker.profiler.agent.DefaultMethodImplInfo;
import com.netcracker.profiler.configuration.Rule;

import java.util.Collection;
import java.util.List;

public interface ConfigurationSPI extends Configuration_06 {
    public Collection<Rule> getRulesForClass(String className, Collection<Rule> rules);

    /**
     * Returns the path of the configuration file was used to load the configuration
     *
     * @return path to the configuration file (relative or absolute)
     */
    public String getConfigFile();

    public List<DefaultMethodImplInfo> getDefaultMethods(String className);
}
