package com.netcracker.persistence.adapters.cloud.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.CloudTableGenerator;
import com.netcracker.persistence.adapters.cloud.cdt.CloudCallsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudCallsDao {

    private static final String INSERT = """
            INSERT INTO calls_%d(time, cpu_time, wait_time, memory_used, duration, non_blocking, queue_wait_duration,
             suspend_duration, calls, transactions, logs_generated, logs_written, file_read, file_written, net_read,
             net_written, namespace, service_name, pod_name, restart_time, method, params, trace_file_index,
             buffer_offset, record_index)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    Connection connection;

    public void insert(CloudCallsEntity entity) {

        String sql = INSERT.formatted(CloudTableGenerator.getTimestampTruncatedToFiveMinutes());

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(entity.time()));
            ps.setLong(2, entity.cpuTime());
            ps.setLong(3, entity.waitTime());
            ps.setLong(4, entity.memoryUsed());
            ps.setLong(5, entity.duration());
            ps.setLong(6, entity.nonBlocking());
            ps.setInt(7, entity.queueWaitDuration());
            ps.setInt(8, entity.suspendDuration());
            ps.setInt(9, entity.calls());
            ps.setLong(10, entity.transactions());
            ps.setInt(11, entity.logsGenerated());
            ps.setInt(12, entity.logsWritten());
            ps.setLong(13, entity.fileRead());
            ps.setLong(14, entity.fileWritten());
            ps.setLong(15, entity.netRead());
            ps.setLong(16, entity.netWritten());
            ps.setString(17, entity.namespace());
            ps.setString(18, entity.serviceName());
            ps.setString(19, entity.podName());
            ps.setTimestamp(20, Timestamp.from(entity.restartTime()));
            ps.setInt(21, entity.method());
            ps.setObject(22, MAPPER.writeValueAsString(entity.params())); // TODO: check it with setString
            ps.setInt(23, entity.traceFileIndex());
            ps.setInt(24, entity.bufferOffset());
            ps.setInt(25, entity.recordIndex());
            ps.executeUpdate();
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during prepare statement to save calls: %s", e.getMessage());
        }
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for calls: %s", e.getMessage());
        }
    }

}
