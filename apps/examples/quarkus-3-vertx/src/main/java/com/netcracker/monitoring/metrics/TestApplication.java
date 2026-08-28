package com.netcracker.monitoring.metrics;

import io.micrometer.core.instrument.*;
import io.quarkus.scheduler.Scheduled;

import jakarta.ws.rs.*;

import java.net.http.HttpClient;
import java.util.*;

@Path("/custom")
@Produces("text/plain")
public class TestApplication {

    private final Helper helper;

    TestApplication(MeterRegistry registry) {
        this.helper = new Helper(registry, 10000);
    }

    @GET
    @Path("prime/{number}")
    public String checkIfPrime(long number) {
        return helper.checkIfPrime(number);
    }

    @GET
    @Path("gauge/{number}")
    public Long checkListSize(long number) {
        return helper.state.updateList(number);
    }

    @GET
    @Path("memory/{mb}")
    public void memory(int mb) {
        helper.generateMegabytes(mb);
    }

    @GET
    @Path("memory/oom")
    public void memory() {
        helper.outOfMemory();
    }

    @GET
    @Path("recursive/{level}")
    public void recursive(int level) {
        helper.recursive(level);
    }

    @GET
    @Path("recursive")
    public void recursive() {
        helper.recursive();
    }

    @GET
    @Path("/err")
    public Map<String, String> errorResponse() {
        return helper.getErrorResponse();
    }

    @GET
    @Path("/health")
    public Map<String, String> healthResponse() {
        return helper.getStatus();
    }

    @POST
    @Path("/health")
    public Map<String, String> healthResponse(@QueryParam(value = "status") String status) {
        return helper.updateStatus(status);
    }

    @Scheduled(every = "10s")
    public void callRandomEndpoint() {
        HttpClient client = HttpClient.newHttpClient();
        helper.callRandomEndpoint(client);
    }

}
