package com.netcracker.persistence.adapters.cloud.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodStatisticsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudPodStatisticsDao {

    private static final String INSERT = """
            INSERT INTO pod_statistics(date, pod_id, pod_name, restart_time, cur_time, data_accumulated, original_accumulated)
            VALUES (?, ?, ?, ?, ?, ?::JSONB, ?::JSONB)
            ON CONFLICT (date, pod_name, restart_time, cur_time)
            DO UPDATE
            SET data_accumulated = EXCLUDED.data_accumulated, original_accumulated = EXCLUDED.original_accumulated
            """;
    private static final String GET_LATEST_POD_STATISTICS_1 = """
            SELECT * FROM pod_statistics
            WHERE pod_id = ANY(?::TEXT[])
            AND cur_time <= ?
            ORDER BY cur_time DESC, restart_time DESC
            """;
    private static final String GET_LATEST_POD_STATISTICS_2 = """
            SELECT * FROM pod_statistics
            WHERE pod_id = ?
            AND cur_time <= ?
            ORDER BY cur_time DESC, restart_time DESC
            LIMIT 1
            """;
    private static final String FIND_LATEST_STAT = """
            SELECT * FROM pod_statistics
            WHERE pod_id = ?
            AND restart_time = ?
            AND date = ANY(?::TIMESTAMP[])
            ORDER BY cur_time DESC
            LIMIT 1
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    Connection connection;

    public void insert(CloudPodStatisticsEntity entity) {

        try (PreparedStatement ps = connection.prepareStatement(INSERT)) {
            ps.setTimestamp(1, Timestamp.from(entity.date()));                          // date [timestamptz]
            ps.setString(2, entity.podId());                                            // pod_id [text]
            ps.setString(3, entity.podName());                                          // pod_name [text]
            ps.setTimestamp(4, Timestamp.from(entity.restartTime()));                   // restart_time [timestamptz]
            ps.setTimestamp(5, Timestamp.from(entity.curTime()));                       // cur_time [timestamptz]
            ps.setObject(6, MAPPER.writeValueAsString(entity.dataAccumulated()));       // data_accumulated [jsonb] DO UPDATE
            ps.setObject(7, MAPPER.writeValueAsString(entity.originalAccumulated()));   // original_accumulated [jsonb] DO UPDATE
            ps.executeUpdate();
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during pod statistics saving: %s", e.getMessage());
        }
    }

    public List<CloudPodStatisticsEntity> find(List<String> podIds, Instant to) {

        // FIXME: Replace with postgres procedure (array of string as parameter) (future)
        String podIdsString = podIds.stream().collect(Collectors.joining(", ", "{", "}"));

        try (var statement = connection.prepareStatement(GET_LATEST_POD_STATISTICS_1)) {
            statement.setString(1, podIdsString);
            statement.setTimestamp(2, Timestamp.from(to));
            return toList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting active pods: %s", e.getMessage());
        }

        return List.of();
    }

    public List<CloudPodStatisticsEntity> find(String podId, Instant to) {

        try (var statement = connection.prepareStatement(GET_LATEST_POD_STATISTICS_2)) {
            statement.setString(1, podId);
            statement.setTimestamp(2, Timestamp.from(to));
            return toList(statement);
        } catch (SQLException e) {
            Log.errorf("error during getting latest pod statistics: %s", e.getMessage());
        }

        return List.of();
    }

    public Optional<CloudPodStatisticsEntity> findLatestStat(String podId, Instant restartTime, List<Instant> dates) {

        String dateList = dates
                .stream()
                .map(d -> Timestamp.from(d).toString())
                .collect(Collectors.joining(", ", "{", "}"));

        try (var statement = connection.prepareStatement(FIND_LATEST_STAT)) {
            statement.setString(1, podId);
            statement.setTimestamp(2, Timestamp.from(restartTime));
            statement.setString(3, dateList);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(toEntity(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during prepare statement to find pod statistics: %s", e.getMessage());
        }

        return Optional.empty();
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            Log.errorf("error during commit transaction for pod statistics: %s", e.getMessage());
        }
    }

    private List<CloudPodStatisticsEntity> toList(PreparedStatement statement) {
        List<CloudPodStatisticsEntity> list = new ArrayList<>();
        // There should be a catch here so that a non-empty list is returned in case of an error
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                list.add(toEntity(rs));
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.errorf("error during getting pod statistics: %s", e.getMessage());
        }
        return list;
    }

    private CloudPodStatisticsEntity toEntity(ResultSet resultSet) throws SQLException, JsonProcessingException {
        return new CloudPodStatisticsEntity(
                resultSet.getTimestamp("date").toInstant(),
                resultSet.getString("pod_id"),
                resultSet.getString("pod_name"),
                resultSet.getTimestamp("restart_time").toInstant(),
                resultSet.getTimestamp("cur_time").toInstant(),
                MAPPER.readValue(resultSet.getString("data_accumulated"), new TypeReference<>() {
                }),
                MAPPER.readValue(resultSet.getString("original_accumulated"), new TypeReference<>() {
                })
        );
    }
}
