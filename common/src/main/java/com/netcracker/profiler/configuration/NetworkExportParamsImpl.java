package com.netcracker.profiler.configuration;

import com.netcracker.profiler.agent.FilterOperator;
import com.netcracker.profiler.agent.NetworkExportParams;
import com.netcracker.profiler.configuration.callfilters.FilterOperatorAnd;

import java.util.List;

public class NetworkExportParamsImpl implements NetworkExportParams {
    private String host;
    private int port;
    private int socketTimeout;
    private List<String> includedParams;
    private List<String> excludedParams;

    private List<String> systemProperties;
    private FilterOperator filter = new FilterOperatorAnd();

    public NetworkExportParamsImpl() {}

    public NetworkExportParamsImpl(String host, int port, int socketTimeout, List<String> includedParams, List<String> excludedParams, List<String> systemProperties) {
        this.host = host;
        this.port = port;
        this.socketTimeout = socketTimeout;
        this.includedParams = includedParams;
        this.excludedParams = excludedParams;
        this.systemProperties = systemProperties;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public int getSocketTimeout() {
        return socketTimeout;
    }

    @Override
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public List<String> getIncludedParams() {
        return includedParams;
    }

    @Override
    public void setIncludedParams(List<String> includedParams) {
        this.includedParams = includedParams;
    }

    @Override
    public List<String> getExcludedParams() {
        return excludedParams;
    }

    @Override
    public void setExcludedParams(List<String> excludedParams) {
        this.excludedParams = excludedParams;
    }

    @Override
    public FilterOperator getFilter() {
        return filter;
    }

    @Override
    public void setFilter(FilterOperator filter) {
        this.filter = filter;
    }

    public List<String> getSystemProperties() {
        return systemProperties;
    }

    public void setSystemProperties(List<String> systemProperties) {
        this.systemProperties = systemProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NetworkExportParamsImpl that = (NetworkExportParamsImpl) o;

        if (port != that.port) return false;
        if (socketTimeout != that.socketTimeout) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (includedParams != null ? !includedParams.equals(that.includedParams) : that.includedParams != null)
            return false;
        if (systemProperties != null ? !systemProperties.equals(that.systemProperties) : that.systemProperties != null)
            return false;
        return excludedParams != null ? excludedParams.equals(that.excludedParams) : that.excludedParams == null;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        result = 31 * result + socketTimeout;
        result = 31 * result + (includedParams != null ? includedParams.hashCode() : 0);
        result = 31 * result + (excludedParams != null ? excludedParams.hashCode() : 0);
        result = 31 * result + (systemProperties != null ? systemProperties.hashCode() : 0);
        return result;
    }
}
