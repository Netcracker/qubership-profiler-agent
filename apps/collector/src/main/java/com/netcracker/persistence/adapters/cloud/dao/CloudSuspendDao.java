package com.netcracker.persistence.adapters.cloud.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.CloudTableGenerator;
import com.netcracker.persistence.adapters.cloud.cdt.CloudSuspendEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudSuspendDao {

    private static final String INSERT = """
            INSERT INTO suspend_%d(date, pod_id, pod_name, restart_time, cur_time, suspend_time)
            VALUES (?, ?, ?, ?, ?, ?::JSONB)
            ON CONFLICT DO NOTHING
            """;

    private static final String FIND = """
            SELECT * FROM suspend
            WHERE date = ANY(?::TIMESTAMP[])
            AND pod_id = ?
            AND restart_time = ?
            AND cur_time >= ?
            AND cur_time <= ?
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    Connection connection;

    public void insert(CloudSuspendEntity entity) {

        String sql = INSERT.formatted(CloudTableGenerator.getTimestampTruncatedToFiveMinutes());

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(entity.date()));                  // date [timestamptz]
            ps.setString(2, entity.podId());                                    // pod_id [text]
            ps.setString(3, entity.podName());                                  // pod_name [text]
            ps.setTimestamp(4, Timestamp.from(entity.restartTime()));           // restart_time [timestamptz]
            ps.setTimestamp(5, Timestamp.from(entity.curTime()));               // cur_time [timestamptz]
            ps.setObject(6, MAPPER.writeValueAsString(entity.suspendTime()));   // suspend_time [jsonb]
            ps.executeUpdate();
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during suspend saving: %s", e.getMessage());
        }
    }

    @Deprecated(forRemoval = true)
    public List<CloudSuspendEntity> find(List<Instant> dates, String podId, Instant restartTime, Instant from, Instant to) {
        return Collections.emptyList();
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for suspend: %s", e.getMessage());
        }
    }

    private CloudSuspendEntity toEntity(String podId, Instant restartTime, ResultSet resultSet) throws SQLException, JsonProcessingException {
        return new CloudSuspendEntity(
                resultSet.getTimestamp("date").toInstant(),
                podId,
                resultSet.getString("pod_name"),
                restartTime,
                resultSet.getTimestamp("cur_time").toInstant(),
                MAPPER.readValue(resultSet.getString("suspend_time"), new TypeReference<>() {
                })
        );
    }
}
