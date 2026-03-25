package com.netcracker.cdt.ui.rest.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.netcracker.cdt.ui.models.UiServiceConfig;
import com.netcracker.cdt.ui.rest.ExceptionMappers;
import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.rest.v2.dto.Responses;
import com.netcracker.cdt.ui.services.CdtCallService;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.common.Time;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.persistence.PersistenceService;
import io.quarkiverse.bucket4j.runtime.RateLimited;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
@Path("/cdt/v2/calls")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CdtCallController {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Inject
    Time time;

    @Inject
    PersistenceService persistence;
    @Inject
    UiServiceConfig config;
    @Inject
    CdtCallService callService;

    /**
     * UI: search calls by query parameters
     */
    @POST
    @Path("/load")
    public Responses.CallsList callsList(Requests.CallsList req) {
        // validation
        if (!req.validate()) {
            throw new ExceptionMappers.InvalidRequest("invalid request, error during validation");
        }
        var search = req.prepareSearchRequest();

        var list = callService.getCallList(search);
        var status = Responses.CallsList.of(list.status());
        var calls = Responses.CallsList.convert(list);
        return new Responses.CallsList(status, calls);

    }

    @POST
    @Path("/stat")
    public Responses.CallsStatistic callsStat(Requests.CallsList req) {
        // validation
        if (!req.validate()) {
            throw new ExceptionMappers.InvalidRequest("invalid request, error during validation");
        }
        var search = req.prepareSearchRequest();
        search = search.overrideLimit(0, 1000);

        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Log.errorf("error during thread sleep: %s", e.getMessage());
        }

        var list = callService.getCallList(search);
        var status = Responses.CallsStatistic.of(list.status());
        var calls = Responses.CallsStatistic.convert(list);
        return new Responses.CallsStatistic(status, calls);

    }

    /**
     * UI: export calls by query parameters
     */
    @GET
    @Path("/export/csv")
    public Response csvExport(HttpServerRequest req,
                              @RestQuery long timeFrom, @RestQuery long timeTo, @RestQuery long durationFrom,
                              @RestQuery String query, @RestQuery String services) {
        var search = exportRequest(timeFrom, timeTo, durationFrom, query, services);
        return downloadExport(req, "csv", search);
    }

    /**
     * UI: export calls by query parameters
     */
    @GET
    @Path("/export/excel")
    public Response xlsExport(HttpServerRequest req,
                              @RestQuery long timeFrom, @RestQuery long timeTo, @RestQuery long durationFrom,
                              @RestQuery String query, @RestQuery String services) {
        var search = exportRequest(timeFrom, timeTo, durationFrom, query, services);
        return downloadExport(req, "excel", search);
    }

    private CallsListRequest exportRequest(long from, long to, long durationFrom, String query, String services) {
        try {
            var timeRange = TimeRange.ofEpochMilli(from, to);
            if (!timeRange.isValid()) {
                throw new ExceptionMappers.InvalidRequest("invalid time range");
            }
            var duration = DurationRange.ofMillis(durationFrom, 30879000);
            if (!duration.isValid()) {
                throw new ExceptionMappers.InvalidRequest("invalid duration range");
            }
            var servicesList = mapper.readValue(services, Requests.Service[].class);
            if (servicesList == null || servicesList.length == 0) {
                throw new ExceptionMappers.InvalidRequest("invalid list of services");
            }
            var search = new CallsListRequest("", -1,
                    timeRange, duration,
                    query, "",
                    Arrays.asList(servicesList),
                    0, config.getMaxExportRows(),
                    0, false); // ts DESC
            return search;
        } catch (JsonProcessingException e) {
            Log.errorf("Could not parse json for list of services: %s", services);
            throw new ExceptionMappers.InvalidRequest("invalid request, error during validation");
        }
    }

    private Response downloadExport(HttpServerRequest req, String csv, CallsListRequest search) {
        var serverAddress = "%s://%s".formatted(req.scheme(), req.host());
        var result = callService.exportCalls(serverAddress, csv, search);
        return Response.ok(result.httpStream())
                .header(HttpHeaders.CONTENT_TYPE, "application/zip")
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=\"cdt.%s.zip\"".formatted(result.fileName()))
                .build();
    }

    /**
     *  Return meta-data statistics (count of meta_params, meta_dictionary) for pod.
     *  Not part of public API
     */
    @GET
    @Path("/meta/{podName}/stat")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getMetaStat(@RestPath String podName) {
        var pod = PodIdRestart.of(podName);

        return Map.of(
                "params", persistence.meta.getParams(pod).size(),
                "dictionary", persistence.meta.getDictionary(pod).size()
        );
    }

    /**
     * Return rows from meta_params for pod.
     * Not part of public API
     */
    @GET
    @Path("/meta/{podName}/params")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ParamsModel> getParameters(@RestPath String podName) {
        var pod = PodIdRestart.of(podName);
        return persistence.meta.getParams(pod);
    }

    /**
     * Return rows from meta_dictionary for pod.
     * Not part of public API
     */
    @GET
    @Path("/meta/{podName}/dictionary")
    @Produces(MediaType.APPLICATION_JSON)
    public List<DictionaryModel> getDictionary(@RestPath String podName) {
        var pod = PodIdRestart.of(podName);
        return persistence.meta.getDictionary(pod);
    }

    /**
     * Return non-empty tags (from meta_dictionary) for pod.
     * Not part of public API
     */
    @GET
    @Path("/meta/{podName}/tags")
    @Produces(MediaType.TEXT_PLAIN)
    public String getTagsList(@RestPath String podName) {
        var pod = PodIdRestart.of(podName);
        List<DictionaryModel> res = persistence.meta.getDictionary(pod);
        List<String> list = res.stream().
                filter(Objects::nonNull).
                map(DictionaryModel::tag).
                sorted().toList();
        return StringUtils.join(list, "\n");
    }

    /**
     * Return all calls in time range for pod.
     * Not part of public API
     */
    @GET
    @Path("/{namespace}/{serviceName}")
    @RateLimited(bucket = "call")
    public Responses.CallsList getCallList(@RestPath String namespace, @RestPath String serviceName,
                                           @RestQuery Instant from, @RestQuery Instant to) {

        int offset = 0;
        int limit = 300;
        var search = new CallsListRequest(
                "system",
                time.currentTimeMillis(),
                TimeRange.of(from, to),
                DurationRange.ofSeconds(1, 100),
                "", "",
                List.of(new Requests.Service(namespace, serviceName)),
                offset, limit,
                0, true); // timestamp

        var list = callService.getCallList(search);

        var status = Responses.CallsList.of(list.status());
        var calls = Responses.CallsList.convert(list);
        return new Responses.CallsList(status, calls);
    }

}
