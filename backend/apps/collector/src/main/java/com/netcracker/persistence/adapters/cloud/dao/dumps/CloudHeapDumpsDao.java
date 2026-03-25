package com.netcracker.persistence.adapters.cloud.dao.dumps;

import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudHeapDumpsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("SqlNoDataSourceInspection")
@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudHeapDumpsDao {

    public static final String GET_HEAP_DUMPS = """
            SELECT * FROM dump_pods
            JOIN heap_dumps ON dump_pods.id = heap_dumps.pod_id
            WHERE heap_dumps.creation_time BETWEEN ? AND ?
            """;

    @Inject
    Connection connection;

    public List<CloudHeapDumpsEntity> find(Instant from, Instant to) {

        List<CloudHeapDumpsEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(GET_HEAP_DUMPS)) {
            statement.setTimestamp(1, Timestamp.from(from));
            statement.setTimestamp(2, Timestamp.from(to));
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(toEntity(rs));
                }
            }
        } catch (SQLException e) {
            Log.error("Failed to find heap dumps", e);
            return Collections.emptyList();
        }
        return list;
    }

    private CloudHeapDumpsEntity toEntity(ResultSet resultSet) throws SQLException {
        return new CloudHeapDumpsEntity(
                resultSet.getString("namespace"),
                resultSet.getString("service_name"),
                resultSet.getString("pod_name"),
                resultSet.getString("handle"),
                resultSet.getTimestamp("creation_time").toInstant(),
                resultSet.getLong("file_size")
        );
    }
}
