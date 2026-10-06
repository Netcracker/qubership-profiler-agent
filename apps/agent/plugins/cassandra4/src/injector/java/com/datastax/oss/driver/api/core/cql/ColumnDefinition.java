package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

public interface ColumnDefinition {
    CqlIdentifier getName();

    DataType getType();
}
