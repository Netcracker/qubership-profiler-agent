package com.netcracker.fixtures.api.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.rest.v2.dto.Responses;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import io.quarkus.logging.Log;
import io.restassured.http.ContentType;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.apache.commons.lang.StringUtils;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipInputStream;

import static com.netcracker.utils.Utils.assertJsonEquals;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

public class Calls extends Common {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public record TestJsonResponse(Responses.CallsList result, String json) {
        public void assertCalls(int expected) {
            assertEquals(expected, result.calls().size());
        }

        public void assertTotal(int expected) {
            assertEquals(expected, result.status().filteredRecords());
        }

        public void assertJson(String expected) {
            assertJsonEquals(expected, json);
        }

    }

    public RequestSpecification exportRequest(String exportType, TimeRange range, DurationRange durationRange, String query, Requests.Service... services) {
        String list = "";
        try {
            list = mapper.writeValueAsString(services);
        } catch (JsonProcessingException e) {
            Log.errorf(e, "invalid list of services");
        }
        return given()
                .contentType(ContentType.JSON)
                .queryParam("timeFrom", range.from().toEpochMilli())
                .queryParam("timeTo", range.to().toEpochMilli())
                .queryParam("durationFrom", durationRange.from().toMillis())
                .queryParam("query", query)
                .queryParam("services", list)
                .when()
                .basePath(String.format("/cdt/v2/calls/export/%s", exportType));
    }


    public RequestSpecification postSearchRequest(String windowId, TimeRange timeRange, DurationRange durationRange, String query, Requests.Service... services) {
        var params = new Requests.CallsList.Parameters(windowId, System.currentTimeMillis());
        var view = new Requests.CallsList.View(10, 1, "ts", true);
        var duration = Requests.DurationMs.of(durationRange);
        var filters = new Requests.CallsList.Filters(timeRange, duration, query, Arrays.asList(services));

        return given()
                .contentType(ContentType.JSON)
                .body(new Requests.CallsList(params, filters, view));
    }

    public RequestSpecification postSortedSearchRequest(String windowId, TimeRange timeRange, DurationRange durationRange, String sortColumn, boolean sortOrder, Requests.Service... services) {
        var params = new Requests.CallsList.Parameters(windowId, System.currentTimeMillis());
        var view = new Requests.CallsList.View(10, 1, sortColumn, sortOrder);
        var duration = Requests.DurationMs.of(durationRange);
        var filters = new Requests.CallsList.Filters(timeRange, duration, "", Arrays.asList(services));

        return given()
                .contentType(ContentType.JSON)
                .body(new Requests.CallsList(params, filters, view));
    }

    public ExtractableResponse<Response> POST(int expectedCode, RequestSpecification req) {
        return POST("/cdt/v2/calls/load", expectedCode, req);
    }

    public TestJsonResponse retrieveData(ExtractableResponse<Response> response) {
        var originalJson = response.body().asString();
        assertNotNull(originalJson);
        var res = response.body().as(Responses.CallsList.class);
        assertNotNull(res);
        return new TestJsonResponse(res, originalJson);
    }

    public record TestZipResponse(Map<String, String> files) {
        public void assertFiles(int expected) {
            assertEquals(expected, files.size());
        }

        public void assertFile(String name, String expected) {
            assertNotNull(files.get(name));
            var actual = files.get(name);
            assertEquals(lines(expected), lines(actual));
        }

        public List<String> lines(String s) {
            var arr = StringUtils.split(s.trim(), "\n");
            var res = new ArrayList<String>(arr.length);
            for (var l : arr) {
                res.add(l.trim());
            }
            return res;
        }
    }

    public TestZipResponse retrieveZip(ExtractableResponse<Response> response) {
        var original = response.body().asInputStream();
        assertNotNull(original);

        var files = new TreeMap<String, String>();
        var res = new TestZipResponse(files);
        var zip = new ZipInputStream(original);
        try {
            while (zip.available() > 0) {
                var e = zip.getNextEntry();
                assertNotNull(e, "could not get another entry");
                var fileName = e.getName();
                assertNull(res.files.get(fileName), "already has file " + fileName);
                var data = zip.readAllBytes();
                assertNotNull(data);
                var s = Strings.fromByteArray(data);
                files.put(fileName, s);
                Log.warnf("data from file %s: %s", fileName, s);
            }
        } catch (IOException ex) {
            assertNull(ex);
        }
        return res;
    }


}
