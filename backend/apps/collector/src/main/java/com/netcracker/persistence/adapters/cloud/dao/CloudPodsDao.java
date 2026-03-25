package com.netcracker.persistence.adapters.cloud.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudPodsDao {

    private static final String INSERT = """
            INSERT INTO pods(pod_id, namespace, service_name, pod_name, active_since, last_restart, last_active, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?::JSONB)
            ON CONFLICT (namespace, service_name, pod_name)
            DO UPDATE SET last_restart = EXCLUDED.last_restart, last_active = EXCLUDED.last_active
            """;
    private static final String SELECT_ALL = """
            SELECT * FROM pods
            """;
    private static final String PODS_WHERE_LAST_ACTIVE_AND_ACTIVE_SINCE = """
            SELECT * FROM pods
            WHERE last_active >= ?
            AND active_since <= ?
            """;
    private static final String DUMP_PODS_WHERE_LAST_ACTIVE = """
                SELECT * FROM dump_pods
                WHERE last_active BETWEEN ? AND ?
                """;
    private static final String PODS_WHERE_LAST_ACTIVE_AND_ACTIVE_SINCE_AND_NAMESPACE_ANY_AND_SERVICE_NAME_ANY = """
            SELECT * FROM pods
            WHERE last_active >= ?
            AND active_since <= ?
            AND namespace = ANY(?::TEXT[])
            AND service_name = ANY(?::TEXT[])
            """;
    private static final String FIND_POD = """
            SELECT * FROM pods
            WHERE namespace = ?
            AND service_name = ?
            AND pod_name = ?
            LIMIT 1
            """;
    private static final String FIND_DUMP_PODS = """
                SELECT * FROM dump_pods
                WHERE namespace = ?
                AND service_name = ?
                AND pod_name = ?
                LIMIT 1
                """;
    private static final String FIND_ALL_SERVICES = """
            SELECT DISTINCT service_name FROM pods
            """;
    private static final String UPDATE_LAST_ACTIVE_POD = """
            UPDATE pods
            SET last_active = ?, last_restart = ?
            WHERE pod_id = ?
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    Connection connection;

    public void insert(CloudPodsEntity entity) {

        Log.infof("insert %s", entity.toString());

        try (PreparedStatement ps = connection.prepareStatement(INSERT)) {
            ps.setString(1, entity.podId());                           // pod_id [text]
            ps.setString(2, entity.namespace());                       // namespace [text]
            ps.setString(3, entity.serviceName());                     // service_name [text]
            ps.setString(4, entity.podName());                         // pod_name [text]
            ps.setTimestamp(5, Timestamp.from(entity.activeSince()));  // active_since [timestamp]
            ps.setTimestamp(6, Timestamp.from(entity.lastRestart()));  // last_restart [timestamp] DO UPDATE
            ps.setTimestamp(7, Timestamp.from(entity.lastActive()));   // last_active [timestamp] DO UPDATE
            ps.setObject(8, MAPPER.writeValueAsString(entity.tags())); // tags [jsonb]
            ps.executeUpdate();
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during pod saving: %s", e.getMessage());
        }
    }

    public void update(String podId, Instant lastActive, Instant lastRestart) {

        try (PreparedStatement ps = connection.prepareStatement(UPDATE_LAST_ACTIVE_POD)) {
            // TODO: lastActive and restartTime equals probably
            ps.setTimestamp(1, Timestamp.from(lastActive));   // last_active [timestamp]
            ps.setTimestamp(2, Timestamp.from(lastRestart));  // last_restart [timestamp]
            ps.setString(3, podId);                           // pod_id [text]
            ps.executeUpdate();
        } catch (SQLException e) {
            Log.errorf("error during prepare statement to update last active of pod: %s", e.getMessage());
        }
    }

    public Optional<CloudPodsEntity> find(String namespace, String serviceName, String podName) {

        try (var statement = connection.prepareStatement(FIND_POD)) {
            statement.setString(1, namespace);
            statement.setString(2, serviceName);
            statement.setString(3, podName);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(toEntity(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during prepare statement to find pod: %s", e.getMessage());
        }

        return Optional.empty();
    }

    public Optional<CloudPodsEntity> findDumpPod(String namespace, String serviceName, String podName) {

        try (var statement = connection.prepareStatement(FIND_DUMP_PODS)) {
            statement.setString(1, namespace);
            statement.setString(2, serviceName);
            statement.setString(3, podName);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(toDumpsEntity(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during prepare statement to find pod: %s", e.getMessage());
        }

        return Optional.empty();
    }

    public List<CloudPodsEntity> find() {

        try (var statement = connection.prepareStatement(SELECT_ALL)) {
            return toList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting pods: %s", e.getMessage());
        }

        return List.of();
    }

    public List<CloudPodsEntity> find(Instant from, Instant to) {

        try (var statement = connection.prepareStatement(PODS_WHERE_LAST_ACTIVE_AND_ACTIVE_SINCE)) {
            statement.setTimestamp(1, Timestamp.from(from));
            statement.setTimestamp(2, Timestamp.from(to));
            return toList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting pods: %s", e.getMessage());
        }

        return List.of();
    }

    public List<CloudPodsEntity> findDumpPods(Instant from, Instant to) {

        try (var statement = connection.prepareStatement(DUMP_PODS_WHERE_LAST_ACTIVE)) {
            statement.setTimestamp(1, Timestamp.from(from));
            statement.setTimestamp(2, Timestamp.from(to));
            return toDumpPodsList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting pods: %s", e.getMessage());
        }

        return List.of();
    }

    public List<CloudPodsEntity> find(List<String> namespaces, List<String> services, Instant from, Instant to) {

        // FIXME: Replace with postgres procedure (array of string as parameter) (future)
        String namespacesParam = namespaces.stream().collect(Collectors.joining(", ", "{", "}"));
        String servicesParam = services.stream().collect(Collectors.joining(", ", "{", "}"));

        try (var statement = connection.prepareStatement(PODS_WHERE_LAST_ACTIVE_AND_ACTIVE_SINCE_AND_NAMESPACE_ANY_AND_SERVICE_NAME_ANY)) {
            statement.setTimestamp(1, Timestamp.from(from));
            statement.setTimestamp(2, Timestamp.from(to));
            statement.setString(3, namespacesParam);
            statement.setString(4, servicesParam);
            return toList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting pods: %s", e.getMessage());
        }

        return List.of();
    }

    public List<CloudPodsEntity> findAllServices() {

        // TODO: change to postgres function
        try (var statement = connection.prepareStatement(FIND_ALL_SERVICES)) {
            return toList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting all services: %s", e.getMessage());
        }

        return List.of();
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for pods: %s", e.getMessage());
        }
    }

    private List<CloudPodsEntity> toList(PreparedStatement statement) {
        List<CloudPodsEntity> list = new ArrayList<>();
        // There should be a catch here so that a non-empty list is returned in case of an error
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                list.add(toEntity(rs));
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during getting pods: %s", e.getMessage());
        }
        return list;
    }

    private List<CloudPodsEntity> toDumpPodsList(PreparedStatement statement) {
        List<CloudPodsEntity> list = new ArrayList<>();
        // There should be a catch here so that a non-empty list is returned in case of an error
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                list.add(toDumpsEntity(rs));
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during getting pods: %s", e.getMessage());
        }
        return list;
    }

    private CloudPodsEntity toEntity(ResultSet resultSet) throws SQLException, JsonProcessingException {
        return new CloudPodsEntity(
                resultSet.getString("pod_id"),
                resultSet.getString("namespace"),
                resultSet.getString("service_name"),
                resultSet.getString("pod_name"),
                resultSet.getTimestamp("active_since").toInstant(),
                resultSet.getTimestamp("last_restart").toInstant(),
                resultSet.getTimestamp("last_active").toInstant(),
                MAPPER.readValue(resultSet.getString("tags"), new TypeReference<>() {
                })
        );
    }

    private CloudPodsEntity toDumpsEntity(ResultSet resultSet) throws SQLException, JsonProcessingException {
        return new CloudPodsEntity(
                resultSet.getString("id"),
                resultSet.getString("namespace"),
                resultSet.getString("service_name"),
                resultSet.getString("pod_name"),
                resultSet.getTimestamp("last_active").toInstant(),
                resultSet.getTimestamp("restart_time").toInstant(),
                resultSet.getTimestamp("last_active").toInstant(),
                new HashMap<String, String>()
        );
    }
}
