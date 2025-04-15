package com.datastax.oss.driver.internal.core.cql;

import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.type.DataType;

@SuppressWarnings("unused")
public class CqlRequestHandler {

    public void sendRequest$profiler(com.datastax.oss.driver.api.core.cql.Statement statement) {

        String query = null;
        String binds = null;

        if (statement instanceof com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement) {
            query = ((DefaultSimpleStatement) statement).getQuery();
        } else if (statement instanceof com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement) {
            // TODO: implement receiving a request from DefaultBatchStatement
            query = "null";
        } else if (statement instanceof com.datastax.oss.driver.internal.core.cql.DefaultBoundStatement) {
            DefaultBoundStatement defaultBoundStatement = (DefaultBoundStatement) statement;
            query = defaultBoundStatement.getPreparedStatement().getQuery();
            binds = parseBinds$profiler(defaultBoundStatement);
        }

        if (!StringUtils.isBlank(query)) {
            Profiler.event(query, "sql");
        }
        if (!StringUtils.isBlank(binds)) {
            Profiler.event(binds, "binds");
        }
    }

    private String parseBinds$profiler(DefaultBoundStatement bound) {
        ColumnDefinitions columnDefinition = bound.getPreparedStatement().getVariableDefinitions();
        StringBuilder bindsBuilder = new StringBuilder();
        if (columnDefinition.size() > 0) {
            for (int i = 0; i < columnDefinition.size(); i++) {
                ColumnDefinition definition = columnDefinition.get(i);
                DataType type = definition.getType();
                CqlIdentifier identifier = definition.getName();
                Object value = bound.getObject(i);
                bindsBuilder.append(type).append(": ").append(identifier).append(": ").append(value).append("\n");
            }
        }
        return bindsBuilder.toString();
    }
}
