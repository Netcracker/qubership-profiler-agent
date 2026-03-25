package com.netcracker.cdt.ui.rest.v2;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.PodEmulator;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.data.PodBinaryData;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.restassured.RestAssured.given;

@QuarkusTest
@TestProfile(BasicAuthProfile.class)
public class BasicAuthTest {
    public static final PodName POD1 = TestHelper.pod("a", "a", 1);

    public static final Instant CT1 = Instant.parse("2023-07-24T05:20:27.000Z"); // for calls

    public static final PodBinaryData DATA = PodBinaryData.TEST_SERVICE;

    @Inject
    TestHelper test;
    static PodEmulator pod1;

    static AtomicBoolean init = new AtomicBoolean(false);

    @BeforeEach
    public void start() throws Exception {
        if (init.compareAndSet(false, true)) { // upload only once per suite
            try (var ignored = test.withTime(CT1)) {
                pod1 = test.startPod(CT1, POD1, DATA);
                pod1.sendStream(StreamType.CALLS);
                pod1.finish();
                pod1.persistStat();
            }
        }
    }

    @Test
    public void correctHttpBasicCreds() {
        given()
                .auth().basic("test_user", "test_password")
                .when().get("/cdt/v2/containers")
                .then()
                .statusCode(200);
    }

    @Test
    public void wrongHttpBasicCreds() {
        given()
                .auth().basic("wrong_user", "wrong_password")
                .when().get("/cdt/v2/containers")
                .then()
                .statusCode(401);
    }

    @Test
    public void noCreds() {
        given()
                .when().get("/cdt/v2/containers")
                .then()
                .statusCode(401);
    }
}

