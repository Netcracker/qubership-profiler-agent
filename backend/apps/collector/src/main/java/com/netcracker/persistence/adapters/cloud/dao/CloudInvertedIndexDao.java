package com.netcracker.persistence.adapters.cloud.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import org.apache.commons.lang.StringUtils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.profiler.model.QueryFilter;
import com.netcracker.profiler.model.DurationUnit;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import static com.netcracker.persistence.utils.Constants.*;
import static com.netcracker.persistence.utils.MiscUtil.*;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudInvertedIndexDao {

    @Inject
    Connection connection;

    public static final String GET_INVERTED_INDEX_TABLES_BY_TIMERANGE = """
                SELECT table_name
                FROM temp_table_inventory
                WHERE table_type = 'inverted_index'
                  AND table_name LIKE ?
                  AND start_time <= ? AND start_time >= ?
                ORDER BY start_time DESC
            """;

    public static final String GET_S3_FILE_IDS = "SELECT file_id FROM %s WHERE value IN (%s)";

    String normalizedInvertedIndexes = normalizeParamList(INVERTED_INDEX_PARAMS);
    DurationUnit ttl = DurationUnit.parseDurationUnit(INVERTED_INDEX_LIFETIME, INVERTED_INDEX_LIFETIME_UNITS,
            DEFAULT_INVERTED_INDEX_LIFETIME);
    DurationUnit granularity = DurationUnit.parseDurationUnit(INVERTED_INDEX_GRANULARITY,
            INVERTED_INDEX_GRANULARITY_UNITS, DEFAULT_INVERTED_INDEX_GRANULARITY);

    /**
     * Retrieves a list of inverted index table names from the `temp_table_inventory` table that match
     * the provided inverted index prefix and fall within the specified time range.
     *
     * This method filters out invalid or non-matching inverted index prefixes based on the
     * {@code normalizedInvertedIndexes} list. It constructs a query using a wildcard match (e.g., {@code i_sql%})
     * and ensures the tables' time ranges intersect with the provided {@link TimeRange}.
     *
     * @param invertedIndex The normalized inverted index name (e.g., {@code sqlmonitor}, {@code traceid}).
     * @param range         The {@link TimeRange} defining the required intersection with table start and end times.
     *
     * @return A list of matching table names ordered by start time descending.
     *         Returns an empty list if no matches are found or an error occurs.
     */
    public List<String> getInvertedIndexTables(String invertedIndex, TimeRange range) {
        List<String> tableNames = new ArrayList<>();

        if (StringUtils.isEmpty(normalizedInvertedIndexes))
            return tableNames;

        if (!normalizedInvertedIndexes.contains(invertedIndex))
            return tableNames;

        try (var stmt = connection.prepareStatement(GET_INVERTED_INDEX_TABLES_BY_TIMERANGE)) {
            stmt.setString(1, "i_" + invertedIndex + "%");
            stmt.setTimestamp(2, Timestamp.from(range.to())); // table ends after range.to
            stmt.setTimestamp(3, Timestamp.from(range.from())); // table starts before range.from
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tableNames.add(rs.getString("table_name"));
                }
            }
        } catch (SQLException e) {
            Log.errorf(e, "Failed to query temp_table_inventory for invertedIndex=%s", invertedIndex);
        }

        return tableNames;
    }

    /**
    * Retrieves a set of S3 file UUIDs by querying inverted index tables based on a query filter and time range.
    *
    * This method performs the following:
    *   - Parses the logical filter expression (e.g., "+request.id val1 AND -trace.id val2") into included keys.
    *   - Normalizes the keys and fetches matching inverted index tables within the specified {@link TimeRange}.
    *   - For each matching table, performs a value-based query and collects matching S3 file UUIDs.
    *   - Stops execution early if a pre-configured timeout threshold is reached.
    *
    * @param queryFilter A string query filter using logical syntax (e.g., "+param val AND -param2 val2").
    * @param range       The {@link TimeRange} to select relevant inverted index tables by timestamp.
    * @return A {@link Set} of matching S3 file {@link UUID}s.
    * @throws SQLException If any SQL errors occur during query execution.
    * @throws JsonProcessingException If there are issues in parsing the filter (in case it was JSON-based).
    */
    public Set<UUID> getS3FileIds(String queryFilter, TimeRange range) throws SQLException, JsonProcessingException {
        Set<UUID> s3FileIds = new HashSet<>();

        QueryFilter filterCondition = QueryFilter.parseQueryFilter(queryFilter);

        Instant startTime = Instant.now();

        for (Map.Entry<String, List<String>> entry : filterCondition.get_included().entrySet()) {
            String invertedIndex = normalizeParam(entry.getKey());
            List<String> values = entry.getValue();
            if (values == null || values.isEmpty())
                continue;

            List<String> tableNames = getInvertedIndexTables(invertedIndex, range);
            if (tableNames.isEmpty())
                continue;

            for (String tableName : tableNames) {
                if (timeOutReached(startTime, REQUEST_TIMEOUT - 1)) {
                    Log.debug("Timeout reached. Stopping further query execution.");
                    return s3FileIds;
                }

                String placeholders = String.join(",", Collections.nCopies(values.size(), "?"));
                String query = String.format(GET_S3_FILE_IDS, tableName, placeholders);

                try (var statement = connection.prepareStatement(query)) {
                    for (int i = 0; i < values.size(); i++) {
                        statement.setString(i + 1, values.get(i));
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            s3FileIds.add(UUID.fromString(rs.getString("file_id")));
                        }
                    }
                } catch (SQLException e) {
                    Log.errorf(e, "Failed querying table %s", tableName);
                }
            }
        }

        return s3FileIds;
    }
}
