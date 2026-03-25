package com.netcracker.common.models.cloud;

import java.sql.Timestamp;

public record CloudStorageFilesModel(String fileName,
                String namespace,
                Integer durationRange,
                Integer size,
                Timestamp createdTime,
                String linkToFile,
                String localFilePath,
                String st) {
}
