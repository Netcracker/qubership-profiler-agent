package com.netcracker.persistence.adapters.cloud;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.models.CloudCallsResult;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;
import com.netcracker.cdt.ui.services.calls.view.CloudCallsList;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.cloud.CloudStorageFilesModel;
import com.netcracker.common.PersistenceType;
import com.netcracker.persistence.CallSequenceLoader;
import com.netcracker.persistence.adapters.cloud.dao.CloudFilesDao;
import com.netcracker.persistence.adapters.cloud.dao.CloudInvertedIndexDao;
import com.netcracker.profiler.model.DurationUnit;

import static com.netcracker.persistence.utils.Constants.INVERTED_INDEX_LIFETIME;
import static com.netcracker.persistence.utils.Constants.INVERTED_INDEX_LIFETIME_UNITS;
import static com.netcracker.persistence.utils.Constants.DEFAULT_INVERTED_INDEX_LIFETIME;

import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudCallSequenceService implements CallSequenceLoader {

    @Inject
    CloudFilesDao cloudFilesDao;

    @Inject
    CloudInvertedIndexDao invertedIndexDao;

    @ConfigProperty(name = "s3.download.cache.dir")
    String s3CacheDir;

    public ReloadTaskState getCallSequence(List<Requests.Service> services, String queryFilter, TimeRange range,
            DurationRange durationRange, ReloadTaskState reloadTaskState) {
        CloudCallsList callsList = CloudCallsList.create();
        List<CloudStorageFilesModel> cloudFiles = List.of();
        try {
            Set<UUID> s3FileIdList = null;
            DurationUnit ttl = null;

            // If a query filter is provided (not empty), fetch the corresponding S3 file
            // IDs using the inverted index DAO.
            if (!queryFilter.isEmpty()) {
                s3FileIdList = invertedIndexDao.getS3FileIds(queryFilter, range);
                ttl = DurationUnit.parseDurationUnit(INVERTED_INDEX_LIFETIME, INVERTED_INDEX_LIFETIME_UNITS,
                        DEFAULT_INVERTED_INDEX_LIFETIME);
                // If the list of S3 file IDs is non-null and not empty, use it to fetch
                // only the relevant cloud files that match the filtered S3 file IDs.
                if (s3FileIdList != null && !s3FileIdList.isEmpty()) {
                    cloudFiles = cloudFilesDao.getCloudFiles(new ArrayList<>(s3FileIdList), services, range,
                            durationRange);
                } else if (TimeRange.delta(range.from(), Instant.now(), ChronoUnit.DAYS) > ttl.amount) {
                    // If there are no matching file IDs or the filter was empty and time range
                    // requested is beyond INVERTED_INDEX_LIFETIME fetch all cloud files for
                    // the given services and time range without applying the file ID filter.
                    cloudFiles = cloudFilesDao.getCloudFiles(services, range, durationRange);
                }
            } else {
                cloudFiles = cloudFilesDao.getCloudFiles(services, range, durationRange);
            }
        } catch (SQLException | JsonProcessingException e) {
            Log.error("Encountered error while getting cloud files: %s".formatted(e.getMessage()));
            reloadTaskState.recordFailure(e);
            return reloadTaskState;
        }
        var calls = new ArrayList<CallRecord>();
        CloudCallsResult cloudCallsResult;
        for (CloudStorageFilesModel cloudFile : cloudFiles) {
            var localFileName = cloudFile.fileName();
            var file = new File(s3CacheDir(), localFileName);
            final String localFilePath = file.getPath();
            if (!file.exists()) {
                try {
                    cloudFilesDao.downloadCloudFile(cloudFile.linkToFile(), localFilePath);
                } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                        | InvalidResponseException | NoSuchAlgorithmException | KeyManagementException | ServerException
                        | XmlParserException
                        | IllegalArgumentException | IOException e) {
                    Log.error("Encountered error while downloading cloud file(%s): %s".formatted(cloudFile.linkToFile(),
                            e.getMessage()));
                    reloadTaskState.recordFailure(e);
                }
            }
            try {
                cloudCallsResult = cloudFilesDao.getCalls(services, queryFilter, range, durationRange, localFilePath);
                calls.addAll(cloudCallsResult.calls());
                reloadTaskState.recordSuccess(cloudCallsResult.parsedCalls(),
                        cloudCallsResult.fetchedCalls(), cloudCallsResult.pods());
            } catch (IOException e) {
                Log.error("Encountered error while getting call sequennces: %s".formatted(e.getMessage()));
                reloadTaskState.recordFailure(e);
            }
        }
        callsList.setCalls(calls);
        reloadTaskState.setCallsList(callsList);
        return reloadTaskState;
    }

    /**
     * Returns directory for downloaded S3 files. It could be default one, could be
     * mounted or could be empty (for tests)
     */
    private File s3CacheDir() {
        var dir = Paths.get(s3CacheDir.isEmpty() ? "./" : s3CacheDir).toFile();

        if (!dir.exists()) {
            if (!dir.mkdir()) {
                Log.warnf("Could not create directory '%s'", s3CacheDir);
            }
        }
        return dir;
    }
}