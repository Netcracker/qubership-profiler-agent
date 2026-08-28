package com.datastax.oss.driver.api.core.cql;

public interface ColumnDefinitions {
    int size();

    ColumnDefinition get(int i);
}
