package com.netcracker.cdt.ui.rest;

import io.quarkus.logging.Log;
import io.quarkus.runtime.LaunchMode;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

import java.time.Instant;

public class ExceptionMappers {

    @ServerExceptionMapper
    public Response mapException(InvalidRequest x) {
        var msg = new Message(Instant.now(), 400,
                "Invalid request", "Invalid request: " + x.message, "");
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON_TYPE).entity(msg).build();
    }

    @ServerExceptionMapper
    public Response mapException(UnknownPod x) {
        var msg = new Message(Instant.now(), 404,
                "Invalid request", "Unknown pod: " + x.name, "");
        return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(msg).build();
    }

    @ServerExceptionMapper
    public Response mapException(UnknownData x) {
        var msg = new Message(Instant.now(), 404,
                "Invalid request", "Not found data for request: " + x.name, "");
        return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON_TYPE).entity(msg).build();
    }

    @ServerExceptionMapper
    public Response mapException(Throwable x) throws Throwable {
        if (LaunchMode.current() == LaunchMode.NORMAL) {
            var code = 500;
            var status = "Invalid request";
            if (x instanceof NotFoundException) {
                code = Response.Status.NOT_FOUND.getStatusCode(); // 404
                status = "Not found";
            } else {
                Log.errorf(x, "error during execution: %s", x.getMessage());
            }
            var msg = new Message(Instant.now(), code, status, x.getMessage(), "");
            return Response.serverError().entity(msg).build();
        } else {
            throw x;
        }
    }

    public static class UnknownData extends RuntimeException {
        public final String name;

        public UnknownData(String name) {
            this.name = name;
        }
    }

    public static class UnknownPod extends RuntimeException {
        public final String name;

        public UnknownPod(String name) {
            this.name = name;
        }
    }

    public static class InvalidRequest extends RuntimeException {
        public final String message;

        public InvalidRequest(String message) {
            this.message = message;
        }
    }

    public record Message(Instant time, int errorCode, String status, String userMessage, String stackTrace) {
    }
}