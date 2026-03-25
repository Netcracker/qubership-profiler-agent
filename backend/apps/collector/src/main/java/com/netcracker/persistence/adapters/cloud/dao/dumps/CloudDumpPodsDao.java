package com.netcracker.persistence.adapters.cloud.dao.dumps;

import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpPodsEntity;
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
public class CloudDumpPodsDao {

    public static final String GET_DUMP_PODS = """
            SELECT *
            FROM dump_pods
            WHERE namespace = ?
              AND service_name = ?
              AND NOT (last_active < ? OR restart_time > ?)
              AND dump_type && ARRAY['top', 'td']::dump_object_type[];
            """;

    @Inject
    Connection connection;

    public List<CloudDumpPodsEntity> find(String namespace, String service, Instant from, Instant to) {

        List<CloudDumpPodsEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(GET_DUMP_PODS)) {
            statement.setString(1, namespace);
            statement.setString(2, service);
            statement.setTimestamp(3, Timestamp.from(from)); // last_active < time_range_from
            statement.setTimestamp(4, Timestamp.from(to)); // restart_time > time_range_to
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(toEntity(rs));
                }
            }
        } catch (SQLException e) {
            Log.error("Failed to find dump pods", e);
            return Collections.emptyList();
        }

        return list;
    }

    private CloudDumpPodsEntity toEntity(ResultSet resultSet) throws SQLException {
        return new CloudDumpPodsEntity(
                resultSet.getString("id"),
                resultSet.getString("namespace"),
                resultSet.getString("service_name"),
                resultSet.getString("pod_name"),
                resultSet.getTimestamp("restart_time").toInstant(),
                resultSet.getTimestamp("last_active").toInstant(),
                (String[]) resultSet.getArray("dump_type").getArray()
        );
    }
}
