package com.netcracker.persistence.adapters.cloud.dao;

import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodRestartsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudPodRestartsDao {

    private static final String INSERT = """
            INSERT INTO pod_restarts(pod_id, namespace, service_name, pod_name, restart_time, active_since, last_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (namespace, service_name, pod_name, restart_time)
            DO UPDATE
            SET last_active = EXCLUDED.last_active
            """;
    private static final String UPDATE_LAST_ACTIVE = """
            UPDATE pod_restarts
            SET last_active = ?
            WHERE pod_id = ?
            """;
    private static final String FIND = """
            SELECT * FROM pod_restarts
            WHERE pod_id = ?
            AND last_active >= ?
            AND active_since <= ?
            LIMIT ?
            """;

    @Inject
    Connection connection;

    public void insert(CloudPodRestartsEntity entity) {

        Log.infof("insert %s", entity.toString());

        try (PreparedStatement ps = connection.prepareStatement(INSERT)) {
            ps.setString(1, entity.podId());                           // pod_id [text]
            ps.setString(2, entity.namespace());                       // namespace [text]
            ps.setString(3, entity.serviceName());                     // service_name [text]
            ps.setString(4, entity.podName());                         // pod_name [text]
            ps.setTimestamp(5, Timestamp.from(entity.restartTime()));  // restart_time [timestamptz]
            ps.setTimestamp(6, Timestamp.from(entity.activeSince()));  // active_since [timestamptz]
            ps.setTimestamp(7, Timestamp.from(entity.lastActive()));   // last_active [timestamptz] DO UPDATE
            ps.executeUpdate();
        } catch (SQLException e) {
            Log.errorf("error during pod restart saving: %s", e.getMessage());
        }
    }

    public void update(String podId, Instant lastActive) {

        try (PreparedStatement ps = connection.prepareStatement(UPDATE_LAST_ACTIVE)) {
            ps.setTimestamp(1, Timestamp.from(lastActive));   // last_active [timestamp]
            ps.setString(2, podId);                           // pod_id [text]
            ps.executeUpdate();
        } catch (SQLException e) {
            Log.errorf("error during prepare statement to update last active pod restart: %s", e.getMessage());
        }
    }

    public List<CloudPodRestartsEntity> find(String podId, Instant lastActive, Instant activeSince, int limit) {

        List<CloudPodRestartsEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(FIND)) {
            statement.setString(1, podId);
            statement.setTimestamp(2, Timestamp.from(lastActive));
            statement.setTimestamp(3, Timestamp.from(activeSince));
            statement.setInt(4, limit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(new CloudPodRestartsEntity(
                            rs.getString("pod_id"),
                            rs.getString("namespace"),
                            rs.getString("service_name"),
                            rs.getString("pod_name"),
                            rs.getTimestamp("restart_time").toInstant(),
                            rs.getTimestamp("active_since").toInstant(),
                            rs.getTimestamp("last_active").toInstant()
                    ));
                }
            }
        } catch (SQLException e) {
            Log.errorf("error during getting pod restarts: %s", e.getMessage());
        }

        return list;
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for pod restart: %s", e.getMessage());
        }
    }
}
