package com.datastax.driver.core;

public interface PreparedStatement {
    public String getQueryString();
    public ColumnDefinitions getVariables();
}
