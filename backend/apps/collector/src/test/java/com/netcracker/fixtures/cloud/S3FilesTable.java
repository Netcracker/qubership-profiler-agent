package com.netcracker.fixtures.cloud;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class S3FilesTable {
    @Inject
    Connection connection;

    final String queryString = "INSERT INTO s3_files (start_time, end_time, file_type, dump_type, namespace, duration_range, " +
            "file_name, status, services, created_time, api_version, rows_count, file_size, " +
            "remote_storage_path, local_file_path, uuid) " +
            "VALUES (?, ?, ?::file_type, ?, ?, ?, ?, ?::file_status, CAST(? AS JSON), ?, ?, ?, ?, ?, ?, ?)";

    public static String randomUuid() {
        SecureRandom secureRandom = new SecureRandom();
        byte[] bytes = new byte[16];
        secureRandom.nextBytes(bytes);
        UUID uuid = UUID.nameUUIDFromBytes(bytes);
        return uuid.toString();
    }
    public void insertRecord(Instant start_time, Instant end_time, String file_type, String dump_type,
                             String namespace, int duration_range, String file_name,
                             String status, String services, Instant created_time,
                             int api_version, int rows_count, long file_size,
                             String remote_storage_path, String local_file_path) throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(queryString)) {
            preparedStatement.setTimestamp(1, Timestamp.from(start_time));
            preparedStatement.setTimestamp(2, Timestamp.from(end_time));
            preparedStatement.setString(3, file_type);
            preparedStatement.setString(4, dump_type);
            preparedStatement.setString(5, namespace);
            preparedStatement.setInt(6, duration_range);
            preparedStatement.setString(7, file_name);
            preparedStatement.setString(8, status);
            preparedStatement.setString(9, services);
            preparedStatement.setTimestamp(10, Timestamp.from(created_time));
            preparedStatement.setInt(11, api_version);
            preparedStatement.setInt(12, rows_count);
            preparedStatement.setLong(13, file_size);
            preparedStatement.setString(14, remote_storage_path);
            preparedStatement.setString(15, local_file_path);
            preparedStatement.setString(16, randomUuid());
            preparedStatement.executeUpdate();
        }
        connection.commit();
    }
}
