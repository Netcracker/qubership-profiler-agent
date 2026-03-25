package com.netcracker.persistence.adapters.cloud.dao;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.models.CloudCallsResult;
import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.cloud.CloudStorageFilesModel;
import com.netcracker.common.models.meta.dict.CallParameters;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.profiler.model.QueryFilter;

import io.minio.DownloadObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import static com.netcracker.persistence.utils.MiscUtil.*;
import static com.netcracker.persistence.utils.Constants.DEFAULT_S3_FILES_LIMIT;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudFilesDao {

    @Inject
    Connection connection;

    @Inject
    MinioClient minioClient;

    @ConfigProperty(name = "s3.bucket-name")
    String bucketName;

    @ConfigProperty(name = "s3.ignore-cert-check")
    boolean ignoreCertCheck;

    public static final String GET_S3_FILES = """
                SELECT * FROM s3_files
                WHERE duration_range >= ? AND duration_range <= ?
                AND (
                     (? <= start_time AND ? >= end_time)
                     OR (
                         (? >= start_time AND ? <= end_time)
                         OR (? >= start_time AND ? <= end_time)
                     )
                )
                AND status = 'completed'
                AND services ??| ?
                AND namespace IN (%s)
                ORDER BY created_time
                LIMIT %s
            """;

    public static final String GET_S3_FILES_BY_IDS = """
                SELECT * FROM s3_files
                WHERE uuid IN (%s)
                AND duration_range >= ? AND duration_range <= ?
                AND (
                     (? <= start_time AND ? >= end_time)
                     OR (
                         (? >= start_time AND ? <= end_time)
                         OR (? >= start_time AND ? <= end_time)
                     )
                )
                AND status = 'completed'
                AND services ??| ?
                AND namespace IN (%s)
                ORDER BY created_time
                LIMIT %s
            """;

    public void downloadCloudFile(String cloudLinkToFile, String localFilePath) throws InvalidKeyException,
            ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException,
            NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException,
            KeyManagementException {
        if (ignoreCertCheck) {
            minioClient.ignoreCertCheck();
        }
        minioClient.downloadObject(DownloadObjectArgs.builder()
                .bucket(bucketName)
                .object(cloudLinkToFile)
                .filename(localFilePath)
                .build());

    }

    public CloudCallsResult getCalls(List<Requests.Service> services, String queryFilter, TimeRange range,
            DurationRange durationRange, String cloudFile) throws IOException {
        Configuration conf = new Configuration();
        int totalRecords = 0;
        CallRecord callRecord;
        QueryFilter condition = QueryFilter.parseQueryFilter(queryFilter);
        List<CallRecord> records = new ArrayList<>();
        Set<PodIdRestart> pods = new HashSet<>();
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader;
        Group record;
        reader = ParquetReader.builder(readSupport, new Path(cloudFile))
                .withConf(conf)
                .build();

        while ((record = reader.read()) != null) {
            totalRecords++;
            long time = record.getLong("time", 0);
            int duration = record.getInteger("duration", 0);

            if (!((range.from().toEpochMilli() <= time && time <= range.to().toEpochMilli())
                    && (durationRange.from().toMillis() <= duration
                            && duration <= durationRange.to().toMillis()))) {
                continue;
            }

            Map<String, List<String>> paramsMap = readGroupToHashMap(record.getGroup("params", 0));

            // if condition.included params are not present in current call record params,
            // skip current call record
            if (!condition.get_included().isEmpty() && !containsValuesInMap(paramsMap, condition.get_included()))
                continue;
            // if condition.excluded params are present in current call record params, skip
            // current call record
            if (!condition.get_excluded().isEmpty() && containsValuesInMap(paramsMap, condition.get_excluded()))
                continue;

            String namespace = record.getString("namespace", 0);
            String service = record.getString("serviceName", 0);
            String podName = record.getString("podName", 0);
            long podRestartTime = record.getLong("restartTime", 0);
            PodIdRestart currentPod = PodIdRestart.getPodInfo(namespace, service, podName, podRestartTime);
            callRecord = new CallRecord(
                    time,
                    duration,
                    record.getLong("nonBlocking", 0),
                    record.getLong("cpuTime", 0),
                    record.getInteger("queueWaitDuration", 0),
                    record.getInteger("suspendDuration", 0),
                    record.getInteger("calls", 0),
                    currentPod,
                    null,
                    record.getString("method", 0),
                    record.getInteger("transactions", 0),
                    record.getLong("memoryUsed", 0),
                    (int) record.getLong("logsGenerated", 0),
                    (int) record.getLong("logsWritten", 0),
                    record.getLong("fileRead", 0),
                    record.getLong("fileWritten", 0),
                    record.getLong("netRead", 0),
                    record.getLong("netWritten", 0),
                    new CallParameters(paramsMap));
            pods.add(currentPod);
            records.add(callRecord);
        }
        reader.close();
        return new CloudCallsResult(records, totalRecords, records.size(), pods);
    }

    public List<CloudStorageFilesModel> getCloudFiles(List<Requests.Service> serviceQuery, TimeRange range,
            DurationRange durationRange)
            throws SQLException, JsonProcessingException {

        List<String> services = serviceQuery.stream()
                .map(Requests.Service::service)
                .collect(Collectors.toList());
        List<String> namespaces = serviceQuery.stream()
                .map(Requests.Service::namespace)
                .collect(Collectors.toList());

        List<CloudStorageFilesModel> cloudFiles = new ArrayList<>();

        /*
         * Description of start/end time SQL statement block logic:
         * 
         * Here is time diagram of 4 scenarios when we need to match record:
         * <==s3_record===> 1. s3 record starts before requested start and ends after
         * requested end (requested wide interval with multiple records in it)
         * <=========> 2. s3 record starts after requested start and ends after
         * requested end (beginning of wide interval from previous case)
         * <=========> 3. s3 record starts before requested start and ends before
         * requested end (end of wide interval from previous case)
         * <====> 4. s3 record starts after requested start and ends before requested
         * end (requested short interval smaller than s3 record)
         * ------------------> time
         * |<start |<end # this line shows start and end of requested interval on the
         * diagram
         * 
         * Case #1 is covered by separate statement '(s3_from <= start_time AND s3_to >=
         * end_time)'
         * All other cases are covered by checking
         * is s3 record start inside requested interval 's3_from >= start_time AND
         * s3_from <= end_time'
         * is s3 record end inside requested interval 's3_to >= start_time AND s3_to <=
         * end_time'
         */
        String query = GET_S3_FILES.formatted(getQuotedStringOfList(namespaces), DEFAULT_S3_FILES_LIMIT);
        try (var statement = connection.prepareStatement(query)) {
            statement.setLong(1, durationRange.from().toMillis());
            statement.setLong(2, durationRange.to().toMillis());
            Timestamp from = Timestamp.from((range.from()));
            Timestamp to = Timestamp.from((range.to()));
            statement.setTimestamp(3, from);
            statement.setTimestamp(4, to);
            statement.setTimestamp(5, from);
            statement.setTimestamp(6, from);
            statement.setTimestamp(7, to);
            statement.setTimestamp(8, to);
            statement.setArray(9, connection.createArrayOf("text", services.toArray()));
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                cloudFiles.add(new CloudStorageFilesModel(
                        resultSet.getString("file_name"),
                        resultSet.getString("namespace"),
                        resultSet.getInt("duration_range"),
                        resultSet.getInt("rows_count"),
                        resultSet.getTimestamp("created_time"),
                        resultSet.getString("remote_storage_path"),
                        resultSet.getString("local_file_path"),
                        "completed"));
            }
        }
        return cloudFiles;
    }

    /**
     * Retrieves a list of s3 files from the database that match the provided S3 file IDs,
     * services, namespaces, time range, and duration range constraints.
     *
     * The method performs the following steps:
     *   - Builds a dynamic SQL query using the given S3 file UUIDs and filters.
     *   - Executes the query with appropriate parameter bindings.
     *   - Maps the result set to a list of {@link CloudStorageFilesModel} objects.
     *
     * @param s3FileIdList     List of S3 file UUIDs to filter the query.
     * @param serviceQuery     List of {@link Requests.Service} containing service name and namespace information.
     * @param range            {@link TimeRange} object specifying the time range for filtering file creation times.
     * @param durationRange    {@link DurationRange} object specifying acceptable duration ranges (in milliseconds).
     *
     * @return List of {@link CloudStorageFilesModel} matching the criteria. Returns an empty list if no UUIDs are provided.
     *
     * @throws SQLException              If a database access error occurs.
     * @throws JsonProcessingException  If any JSON serialization/deserialization errors occur.
     */
    public List<CloudStorageFilesModel> getCloudFiles(List<UUID> s3FileIdList,
            List<Requests.Service> serviceQuery,
            TimeRange range,
            DurationRange durationRange) throws SQLException, JsonProcessingException {
        List<String> services = serviceQuery.stream()
                .map(Requests.Service::service)
                .collect(Collectors.toList());
        List<String> namespaces = serviceQuery.stream()
                .map(Requests.Service::namespace)
                .collect(Collectors.toList());

        List<CloudStorageFilesModel> cloudFiles = new ArrayList<>();

        if (s3FileIdList == null || s3FileIdList.isEmpty()) {
            return cloudFiles; // Return empty list if no UUIDs provided
        }

        // Build placeholder string (?, ?, ?, ...)
        String placeholders = String.join(", ", Collections.nCopies(s3FileIdList.size(), "?"));
        String query = GET_S3_FILES_BY_IDS.formatted(placeholders, getQuotedStringOfList(namespaces), DEFAULT_S3_FILES_LIMIT);

        try (var statement = connection.prepareStatement(query)) {
            // Set each UUID as a parameter
            int i = 0;
            for (i = 0; i < s3FileIdList.size(); i++) {
                statement.setObject(i + 1, s3FileIdList.get(i));
            }
            statement.setLong(i + 1, durationRange.from().toMillis());
            statement.setLong(i + 2, durationRange.to().toMillis());
            Timestamp from = Timestamp.from((range.from()));
            Timestamp to = Timestamp.from((range.to()));
            statement.setTimestamp(i + 3, from);
            statement.setTimestamp(i + 4, to);
            statement.setTimestamp(i + 5, from);
            statement.setTimestamp(i + 6, from);
            statement.setTimestamp(i + 7, to);
            statement.setTimestamp(i + 8, to);
            statement.setArray(i + 9, connection.createArrayOf("text", services.toArray()));

            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                cloudFiles.add(new CloudStorageFilesModel(
                        resultSet.getString("file_name"),
                        resultSet.getString("namespace"),
                        resultSet.getInt("duration_range"),
                        resultSet.getInt("rows_count"),
                        resultSet.getTimestamp("created_time"),
                        resultSet.getString("remote_storage_path"),
                        resultSet.getString("local_file_path"),
                        "completed"));
            }
        }

        return cloudFiles;
    }
}
