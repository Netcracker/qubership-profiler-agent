package com.netcracker.persistence.adapters.cloud.dao.dumps;

import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpObjectsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudDumpObjectsDao {

    public static final String GET_DUMP_OBJECTS_PODS = """
            SELECT * FROM dump_objects_%s
            WHERE creation_time BETWEEN ? AND ?
            """;

    public static final String GET_DUMP_OBJECTS_PODS_2 = "SELECT * FROM get_availability_dump_objects(?, ?, ?, ?)";

    @Inject
    Connection connection;

    public List<CloudDumpsEntity> find2(String namespace, String serviceName, Instant from, Instant to) {
        List<CloudDumpsEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(GET_DUMP_OBJECTS_PODS_2)) {
            statement.setString(1, namespace);
            statement.setString(2, serviceName);
            statement.setTimestamp(3, Timestamp.from(from));
            statement.setTimestamp(4, Timestamp.from(to));
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(toCloudDumpEntity(rs));
                }
            }
        } catch (SQLException e) {
            Log.errorf("error during getting dump objects: %s", e.getMessage());
        }
        return list;
    }

    public List<CloudDumpObjectsEntity> find(Instant from, Instant to) {

        // Calculate hourly timestamps
        List<Long> hourlyTimestamps = new ArrayList<>();
        Instant current = from.truncatedTo(ChronoUnit.HOURS);
        while (current.isBefore(to) || current.equals(to)) {
            hourlyTimestamps.add(current.getEpochSecond());
            current = current.plus(1, ChronoUnit.HOURS);
        }
        Log.info("timestamps: " + Arrays.toString(hourlyTimestamps.toArray()));

        StringBuilder queryBuilder = new StringBuilder();
        boolean first = true;
        for (long timestamp : hourlyTimestamps) {
            if (!first) {
                queryBuilder.append(" UNION ALL ");
            }
            queryBuilder.append("SELECT * FROM dump_objects_").append(timestamp)
                    .append(" WHERE creation_time BETWEEN ? AND ?");
            first = false;
        }

        String query = queryBuilder.toString();
        Log.info("Executing query: " + query);

        List<CloudDumpObjectsEntity> list = new ArrayList<>();
        try (var statement = connection.prepareStatement(query)) {
            int paramIndex = 0;
            while (paramIndex < hourlyTimestamps.size() * 2) {
                statement.setTimestamp(++paramIndex, Timestamp.from(from));
                statement.setTimestamp(++paramIndex, Timestamp.from(to));
            }

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    list.add(toEntity(rs));
                }
            }
        } catch (SQLException e) {
            Log.errorf("Error during getting dump objects: %s", e.getMessage());
        }

        // // Execute queries
        // List<CloudDumpObjectsEntity> list = new ArrayList<>();
        // for (long timestamp : hourlyTimestamps) {
        //     try (var statement = connection.prepareStatement(String.format(GET_DUMP_OBJECTS_PODS, timestamp))) {
        //     statement.setTimestamp(1, Timestamp.from(from));
        //     statement.setTimestamp(2, Timestamp.from(to));
        //         try (ResultSet rs = statement.executeQuery()) {
        //             while (rs.next()) {
        //                 list.add(toEntity(rs));
        //             }
        //         }
        //     } catch (SQLException e) {
        //         Log.errorf("error during getting dump objects: %s", e.getMessage());
        //     }
        // }

        return list;
    }

    private CloudDumpsEntity toCloudDumpEntity(ResultSet resultSet) throws SQLException {
        return new CloudDumpsEntity(
                resultSet.getString("pod_name"),
                resultSet.getTimestamp("start_time").toInstant(),
                resultSet.getString("dump_type"),
                resultSet.getTimestamp("data_available_from").toInstant(),
                resultSet.getTimestamp("data_available_to").toInstant()
        );
    }

    private CloudDumpObjectsEntity toEntity(ResultSet resultSet) throws SQLException {
        return new CloudDumpObjectsEntity(
                resultSet.getString("id"),
                resultSet.getString("pod_id"),
                resultSet.getTimestamp("creation_time").toInstant(),
                resultSet.getLong("file_size"),
                resultSet.getString("dump_type")
        );
    }
}
