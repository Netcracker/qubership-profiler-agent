package com.netcracker.fixtures.api.v2;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Downloads extends Common {

    public record TestResponse(byte[] blob) {
        public void assertSize(long expected) {
            assertEquals(expected, blob.length);
        }

        public void assertSame(byte[] expected) {
            assertThat(blob, is(expected));
        }
    }

    public RequestSpecification zipRequest(TimeRange range, PodIdRestart pod, StreamType type) {
        return given()
                .queryParam("timeFrom", range.from().toEpochMilli())
                .queryParam("timeTo", range.to().toEpochMilli())
                .when()
                .basePath(String.format("/cdt/v2/dumps/%s/%s/download", pod.oldPodName(), type.getName()));
    }

    public RequestSpecification fileRequest(PodIdRestart pod, StreamType type, int seqId) {
        return given()
                .when()
                .basePath(String.format("/cdt/v2/dumps/%s/%s/%d", pod.oldPodName(), type.getName(), seqId));
    }

    public RequestSpecification deleteRequest(PodIdRestart pod, StreamType type, int seqId) {
        return given()
                .when()
                .basePath(String.format("/cdt/v2/dumps/%s/%s/%d", pod.oldPodName(), type.getName(), seqId));
    }

    public ExtractableResponse<Response> DELETE(int expectedCode, RequestSpecification req) {
        return req
                .delete()
                .then()
                .statusCode(expectedCode)
                .extract();
    }

    public TestResponse retrieveData(ExtractableResponse<Response> response) {
        var originalJson = response.body().asString();
        assertNotNull(originalJson);
        var res = response.body().asByteArray();
        assertNotNull(res);
        return new TestResponse(res);
    }

}
