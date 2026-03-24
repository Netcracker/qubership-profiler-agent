package com.netcracker.persistence.adapters.cloud.dao;

import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.CloudDictionaryEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudDictionaryDao {

    private static final String INSERT = """
            INSERT INTO dictionary(pod_id, pod_name, restart_time, position, tag)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;
    public static final String GET_DICTIONARY = """
            SELECT position, tag
            FROM dictionary
            WHERE pod_id = ?
            AND restart_time = ?
            """;
    public static final String GET_DICTIONARY_2 = """
            SELECT position, tag
            FROM dictionary
            WHERE pod_id = ?
            AND restart_time = ?
            AND position = ANY(?::INT[])
            """;

    @Inject
    Connection connection;

    public void insert(CloudDictionaryEntity entity) {

        try (PreparedStatement ps = connection.prepareStatement(INSERT)) {
            ps.setString(1, entity.podId());                            // pod_id [text]
            ps.setString(2, entity.podName());                          // pod_name [text]
            ps.setTimestamp(3, Timestamp.from(entity.restartTime()));   // restart_time [timestamptz]
            ps.setInt(4, entity.position());                            // position [integer]
            ps.setString(5, entity.tag());                              // tag [text]
            ps.executeUpdate();
        } catch (SQLException e) {
            Log.errorf("error during dictionary saving: %s", e.getMessage());
        }
    }

    public List<CloudDictionaryEntity> find(String podId, Instant restartTime) {

        List<CloudDictionaryEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(GET_DICTIONARY)) {
            statement.setString(1, podId);
            statement.setTimestamp(2, Timestamp.from(restartTime));
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(toEntity(podId, restartTime, rs));
                }
            }
        } catch (SQLException e) {
            Log.errorf("error during dictionary getting: %s", e.getMessage());
        }
        return list;
    }

    public List<CloudDictionaryEntity> find(String podId, Instant restartTime, List<Integer> positions) {

        // FIXME: find better solution
        String positionsString = positions
                .stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", ", "{", "}"));

        List<CloudDictionaryEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(GET_DICTIONARY_2)) {
            statement.setString(1, podId);
            statement.setTimestamp(2, Timestamp.from(restartTime));
            statement.setString(3, positionsString);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(toEntity(podId, restartTime, rs));
                }
            }
        } catch (SQLException e) {
            Log.errorf("error during dictionary getting: %s", e.getMessage());
        }
        return list;
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for dictionary: %s", e.getMessage());
        }
    }

    private CloudDictionaryEntity toEntity(String podId, Instant restartTime, ResultSet resultSet) throws SQLException {
        return new CloudDictionaryEntity(
                podId,
                resultSet.getString("pod_name"),
                restartTime,
                resultSet.getInt("position"),
                resultSet.getString("tag")
        );
    }
}
