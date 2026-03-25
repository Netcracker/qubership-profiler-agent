package com.netcracker.fixtures.api.v2;

import com.netcracker.cdt.ui.rest.v2.dto.Responses;
import com.netcracker.common.models.TimeRange;
import io.quarkus.logging.Log;
import io.restassured.http.ContentType;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.Arrays;
import java.util.List;

import static com.netcracker.utils.Utils.assertJsonEquals;
import static com.netcracker.utils.Utils.toJson;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Containers extends Common {

    public record TestResponse(List<Responses.Container> list, String json, String original) {
        public void assertSize(int expected) {
            assertEquals(expected, list.size());
        }

        public void assertJson(String expected) {
            assertJsonEquals(expected, json);
        }
    }

    public String GetVersion(int expectedCode) {
        return given()
                .when()
                .get("/cdt/v2/version")
                .then()
                .statusCode(expectedCode)
                .extract().body().asString();
    }

    public ExtractableResponse<Response> GetContainers() {
        return given()
                .contentType(ContentType.JSON)
                .when()
                .get("/cdt/v2/containers")
                .then()
                .statusCode(200)
                .extract();
    }

    public ExtractableResponse<Response> PostContainers(int expectedCode, RequestSpecification req) {
        return req
                .when()
                .post("/cdt/v2/containers")
                .then()
                .statusCode(expectedCode)
                .extract();
    }

    public RequestSpecification postContainersRequest(TimeRange range, int limit, int page) {
        return given()
                .contentType(ContentType.JSON)
                .queryParam("timeFrom", range.from().toEpochMilli())
                .queryParam("timeTo", range.to().toEpochMilli())
                .queryParam("limit", limit)
                .queryParam("page", page);
    }

    public TestResponse retrieveData(ExtractableResponse<Response> response) {
        // retrieve original json
        var originalJson = response.body().asString();
        assertNotNull(originalJson);
        // unmarshall data
        var s = response.body().asString();
        Log.debugf("response: %s", s);
        var res = response.body().as(Responses.Container[].class);
        assertNotNull(res);
        // skip services from other tests
        var list = Arrays.stream(res).filter(c -> c.namespace().contains("CdtControllerTest")).toList();
        // provide json with filtered data
        var json = toJson(list);
        return new TestResponse(list, json, originalJson);
    }
}
