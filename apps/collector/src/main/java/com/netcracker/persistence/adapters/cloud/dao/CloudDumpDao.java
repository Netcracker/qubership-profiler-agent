package com.netcracker.persistence.adapters.cloud.dao;

import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.CloudDumpEntity;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

// Moved to CloudDumpsObjectEntity and CloudHeapDumpsEntity
@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
@Deprecated
public class CloudDumpDao {
    @Inject
    Connection connection;

    public List<CloudDumpEntity> find(List<String> podIds, Instant from, Instant to, String dumpType) {
        try {
            var TABLE_NAME = "heap_dumps";
            if (dumpType != "heap") {
                TABLE_NAME = "dump_objects";
            }
            Array podIdArray = connection.createArrayOf("uuid", podIds.toArray());
            PreparedStatement statement = connection
                    .prepareStatement(String.format("SELECT * FROM %s WHERE pod_id = ANY(?) AND creation_time BETWEEN ? AND ?", TABLE_NAME));
            statement.setArray(1, podIdArray);
            statement.setTimestamp(2, Timestamp.from(from));
            statement.setTimestamp(3, Timestamp.from(to));
            return toList(statement, dumpType);
        } catch (SQLException e) {
            Log.error("Failed to find dumps", e);
            return Collections.emptyList();
        }
    }

    public int count(String dumpType) {
        try {
            var TABLE_NAME = "heap_dumps";
            if (dumpType != "heap") {
                TABLE_NAME = "dump_objects_";
            }
            PreparedStatement statement = connection
                    .prepareStatement(String.format("SELECT COUNT(*) AS total_rows FROM %s", TABLE_NAME));
            ResultSet rs = statement.executeQuery();
            return rs.getInt("total_rows");
        } catch (SQLException e) {
            Log.error("Failed to find dumps", e);
            return 0;
        }
    }

    private List<CloudDumpEntity> toList(PreparedStatement statement, String dumpType) {
        List<CloudDumpEntity> list = new ArrayList<>();
        // There should be a catch here so that a non-empty list is returned in case of
        // an error
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                list.add(toEntity(rs, dumpType));
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during getting dumps: %s", e.getMessage());
        }
        return list;
    }

    private CloudDumpEntity toEntity(ResultSet resultSet, String dumpType)
            throws SQLException, JsonProcessingException {
        if (dumpType == "heap") {
            return new CloudDumpEntity(
                    resultSet.getString("handle"),
                    resultSet.getString("pod_id"),
                    resultSet.getTimestamp("creation_time").toInstant(),
                    resultSet.getLong("file_size"),
                    dumpType);
        }
        return new CloudDumpEntity(
                resultSet.getString("id"),
                resultSet.getString("pod_id"),
                resultSet.getTimestamp("creation_time").toInstant(),
                resultSet.getLong("file_size"),
                resultSet.getString("dump_type"));
    }
}
