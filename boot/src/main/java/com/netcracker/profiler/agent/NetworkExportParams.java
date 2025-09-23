package com.netcracker.profiler.agent;

import java.util.List;

public interface NetworkExportParams {
    String getHost();

    void setHost(String host);

    int getPort();

    void setPort(int port);

    int getSocketTimeout();

    void setSocketTimeout(int socketTimeout);

    List<String> getIncludedParams();

    void setIncludedParams(List<String> includedParams);

    List<String> getExcludedParams();

    void setExcludedParams(List<String> excludedParams);

    FilterOperator getFilter();

    void setFilter(FilterOperator filter);

    List<String> getSystemProperties();

    void setSystemProperties(List<String> systemProperties);
}
