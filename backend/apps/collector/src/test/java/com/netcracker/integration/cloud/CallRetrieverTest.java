package com.netcracker.integration.cloud;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;
import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.meta.dict.CallParameters;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.fixtures.cloud.S3FilesTable;

import org.apache.groovy.util.Maps;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import com.netcracker.fixtures.cloud.MinioUtils;
import com.netcracker.integration.Profiles;
import com.netcracker.persistence.PersistenceService;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import io.quarkus.test.junit.TestProfile;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import static org.junit.jupiter.api.Assertions.*;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@QuarkusTest
@TestProfile(Profiles.CloudTest.class)
@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(SAME_THREAD)
public class CallRetrieverTest {
        @Inject
        PersistenceService persistence;

        @Inject
        MinioUtils minioUtils;

        @Inject
        S3FilesTable s3FilesTable;

        private PodIdRestart getPodByService(String service) {
                return PodIdRestart.getPodInfo("test.namespace-1", service, "test.service-0-8xvrywyhvu-fyf9b", 1729296000000L);
        }

        @Test
        public void sampleTest() throws SQLException {
                String fileDir = "2024/10/20/23/test.namespace-1-90s.parquet";
                String fileName = "test.namespace-1-90s.parquet";
                String method = "void com.netcracker.cdt.uiservice.UiServiceApplication.main(java.lang.String[]) (UiServiceApplication.java:58) [escui.jar!/BOOT-INF/classes]";
                var callParams = new CallParameters(Maps.of("thread", List.of("main")));

                var expectedCallRecords = List.of(
                        new CallRecord(1729780536553L, 93906, 0, 74993, 0, 44364, 416,
                                getPodByService("test.service-0"),
                                null, method, 102, 0, 0, 0, 643899, 6935, 0, 3327174,
                                callParams),
                        new CallRecord(1729780558943L, 93906, 0, 5735, 0, 6467, 148,
                                getPodByService("test.service-0"),
                                null, method, 252, 0, 0, 0, 643899, 6935, 0, 4158557,
                                callParams),
                        new CallRecord(1729780565333L, 93906, 0, 72412, 0, 33220, 471,
                                getPodByService("test.service-0"),
                                null, method,332, 0, 0, 0, 643899, 6935, 0, 1839667,
                                callParams)
                );

                minioUtils.uploadFileAndGetPath(fileDir, fileName);
                s3FilesTable.insertRecord(Instant.parse("2024-10-24T14:00:00.00Z"), Instant.parse("2024-10-24T20:00:00.00Z"), "calls", "", "test.namespace-1",
                                90000,
                                "test.namespace-1-90s.parquet", "completed",
                                "[\"test.service-0\"]",
                                Instant.parse("2024-10-24T17:47:32.29Z"),
                                1,
                                78, 58203, fileDir, fileName);

                var services = List.of(new Requests.Service("test.namespace-1", "test.service-0"));
                var range = TimeRange.ofEpochMilli(1729780536552L, 1729780570000L); // 2024-10-24T14:35:36Z - 2024-10-24T14:36:10Z
                var durationRange = DurationRange.ofMillis(0, 93907);

                var taskState = new ReloadTaskState("test", 0);
                taskState = persistence.cloud.getCallSequence(services, "", range, durationRange, taskState);
                taskState.finish();

                Log.info(taskState.getStatus(0).toString());
                assertFalse(taskState.getCallsList().isEmpty());
                assertEquals(expectedCallRecords, taskState.getCallsList().sortCalls(0, true).all());
        }
}
