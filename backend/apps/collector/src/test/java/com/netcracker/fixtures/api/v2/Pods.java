package com.netcracker.fixtures.api.v2;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.rest.v2.dto.Responses;
import com.netcracker.common.models.TimeRange;
import io.restassured.http.ContentType;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.Arrays;
import java.util.List;

import static com.netcracker.utils.Utils.assertJsonEquals;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Pods extends Common {

    public record TestResponse(List<Responses.Pod> list, String json) {
        public void assertPods(int expected) {
            assertEquals(expected, list.size());
        }

        public void assertJson(String expected) {
            assertJsonEquals(expected, json);
        }
    }

    public RequestSpecification postServiceRequest(TimeRange range, String query, Requests.Service... services) {
        List<Requests.Service> list = Arrays.asList(services);
        return given()
                .contentType(ContentType.JSON)
                .body(new Requests.Services(range, query, list));
    }

    public ExtractableResponse<Response> POST(int expectedCode, RequestSpecification req) {
        return POST("/cdt/v2/services", expectedCode, req);
    }

    public TestResponse retrieveData(ExtractableResponse<Response> response) {
        var originalJson = response.body().asString();
        assertNotNull(originalJson);
        var res = response.body().as(Responses.Pod[].class);
        assertNotNull(res);
        return new TestResponse(Arrays.asList(res), originalJson);
    }


}
