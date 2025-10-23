package com.netcracker.fixtures.api.v2;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
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
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HeapDumps extends Common {
    public final Downloads download = new Downloads();

    public record TestResponse(List<Responses.HeapDumpRecord> list, String json) {
        public void assertReports(int expected) {
            assertEquals(expected, list.size());
        }

        public void assertJson(String expected) {
            assertJsonEquals(expected, json);
        }
    }

    public RequestSpecification request(TimeRange range, String query, int limit, int page) {
        return given()
                .contentType(ContentType.JSON)
                .body(new Requests.ServicePod(range, query, limit, page));
    }

    public ExtractableResponse<Response> POST(int expectedCode, RequestSpecification req) {
        return POST("/cdt/v2/heaps", expectedCode, req);
    }

    public TestResponse retrieveData(ExtractableResponse<Response> response) {
        var originalJson = response.body().asString();
        assertNotNull(originalJson);
        var res = response.body().as(Responses.HeapDumpRecord[].class);
        assertNotNull(res);
        Log.infof("got response %d bytes with %d heap records", originalJson.length(), res.length);
        return new TestResponse(Arrays.asList(res), originalJson);
    }

}
