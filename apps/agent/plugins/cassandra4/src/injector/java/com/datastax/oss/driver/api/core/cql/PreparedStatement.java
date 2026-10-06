package com.datastax.oss.driver.api.core.cql;

public interface PreparedStatement {
    String getQuery();

    ColumnDefinitions getVariableDefinitions();
}
