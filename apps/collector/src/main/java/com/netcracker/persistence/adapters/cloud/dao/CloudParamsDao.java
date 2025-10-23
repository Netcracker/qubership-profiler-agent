package com.netcracker.persistence.adapters.cloud.dao;

import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.CloudParamsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudParamsDao {

    private static final String INSERT = """
            INSERT INTO params(pod_id, pod_name, restart_time, param_name, param_index, param_list, param_order, signature)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;
    private static final String GET = """
            SELECT * FROM params
            WHERE pod_id = ?
            """;

    @Inject
    Connection connection;

    public void insert(CloudParamsEntity entity) {

        try (PreparedStatement ps = connection.prepareStatement(INSERT)) {
            ps.setString(1, entity.podId());                            // pod_id [text]
            ps.setString(2, entity.podName());                          // pod_name [text]
            ps.setTimestamp(3, Timestamp.from(entity.restartTime()));   // restart_time [timestamptz]
            ps.setString(4, entity.paramName());                        // param_name [text]
            ps.setBoolean(5, entity.paramIndex());                      // param_index [boolean]
            ps.setBoolean(6, entity.paramList());                       // param_list [boolean]
            ps.setInt(7, entity.paramOrder());                          // param_order [integer]
            ps.setString(8, entity.signature());                        // signature [text]
            ps.executeUpdate();
        } catch (SQLException e) {
            Log.errorf("error during params saving: %s", e.getMessage());
        }
    }

    public List<CloudParamsEntity> find(String podId) {

        List<CloudParamsEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(GET)) {
            statement.setString(1, podId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    list.add(new CloudParamsEntity(
                            podId,
                            resultSet.getString("pod_name"),
                            resultSet.getTimestamp("restart_time").toInstant(),
                            resultSet.getString("param_name"),
                            resultSet.getBoolean("param_index"),
                            resultSet.getBoolean("param_list"),
                            resultSet.getInt("param_order"),
                            resultSet.getString("signature")
                    ));
                }
            }
        } catch (SQLException e) {
            Log.errorf("error during params getting: %s", e.getMessage());
        }

        return list;
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for params: %s", e.getMessage());
        }
    }
}
