package com.netcracker.fixtures.api.v2;

import io.quarkus.logging.Log;
import io.restassured.http.ContentType;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import static io.restassured.RestAssured.given;

abstract class Common {

    public RequestSpecification jsonRequest(String json) {
        return given()
                .contentType(ContentType.JSON)
                .body(json);
    }


    public ExtractableResponse<Response> GET(int expectedCode, RequestSpecification req) {
        Log.infof("send GET request");
        return req
                .get()
                .then()
                .statusCode(expectedCode)
                .extract();
    }

    public ExtractableResponse<Response> POST(String path, int expectedCode, RequestSpecification req) {
        Log.infof("send POST request: %s", path);
        return req
                .when()
                .post(path)
                .then()
                .statusCode(expectedCode)
                .extract();
    }

}
