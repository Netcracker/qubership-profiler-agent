package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class DefaultBoundStatement {
    public native PreparedStatement getPreparedStatement();

    public native Object getObject(int i);
}
