package com.netcracker.cdt.ui.rest.v2;

import com.netcracker.cdt.ui.rest.ExceptionMappers;
import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.rest.v2.dto.Responses;
import com.netcracker.cdt.ui.rest.v2.dto.responses.DumpRecord;
import com.netcracker.cdt.ui.services.CdtCallService;
import com.netcracker.cdt.ui.services.CdtDumpsService;
import com.netcracker.cdt.ui.services.CdtPodsService;
import com.netcracker.common.Time;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;

import java.util.Collection;
import java.util.List;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
@Path("/cdt/v2/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CdtController {

    @ConfigProperty(name = "cdt.version", defaultValue = "0.0.1")
    String VERSION;

    @ConfigProperty(name = "service.type", defaultValue = "unknown")
    String serviceType;

    @Inject
    Time time;
    @Inject
    CdtPodsService podsService;
    @Inject
    CdtDumpsService dumpsService;
    @Inject
    CdtCallService callService;

    /**
     *
     */
    @GET
    @Path("/version")
    public String getVersion() {
        return VERSION;
    }

    /**
     *
     */
    @GET
    @Path("/containers")
    public List<Responses.Container> getNamespaces() {
        Log.infof("service: %s", serviceType);
        var namespaces = podsService.getNamespaces();
        return namespaces.stream()
                .map(n -> Responses.Container.of(time.now(), n))
                .toList();
    }

    /**
     *
     */
    @POST
    @Path("/containers")
    public Collection<Responses.Container> searchNamespaces(@RestQuery long timeFrom, @RestQuery long timeTo, @RestQuery int limit, @RestQuery int page) {
        // validation
        var range = TimeRange.ofEpochMilli(timeFrom, timeTo);
        if (!range.isValid()) {
            throw new ExceptionMappers.InvalidRequest("invalid time range");
        }
        if (page <= 0 || limit <= 0 || limit > 10000) {
            throw new ExceptionMappers.InvalidRequest("invalid paging parameters");
        }

        // TODO add paging
        var namespaces = podsService.getNamespaces();
        // convert to DTO
        return namespaces.stream()
                .map(n -> Responses.Container.of(time.now(), n))
                .toList();
    }

    /**
     * Retrieve all active services in specified time range in one fetch
     */
    @POST
    @Path("/services")
    public Collection<Responses.Pod> getServices(Requests.Services req) {
        // validation
        if (!req.timeRange().isValid()) {
            throw new ExceptionMappers.InvalidRequest("invalid time range");
        }
        // it's ok to don't have 'query' search
        // it's OK if the list of services is empty (look for all)

        // TODO add services to search
        var pods = podsService.getActivePods(req.timeRange(), req.getFilter());
        // convert to DTO
        return pods.stream()
                .map(p -> Responses.Pod.of(p, p.getTagValues()))
                .toList();
    }

    /**
     * Returns information about the pod on the Pods Info tab when one of the services is expanded to the pod
     */
    @POST
    @Path("/namespaces/{namespace}/services/{service}/dumps")
    public Collection<DumpRecord> getDumps(@RestPath String namespace, @RestPath String service, Requests.ServicePod req) {
        // Validation
        if (!req.timeRange().isValid()) {
            throw new ExceptionMappers.InvalidRequest("invalid time range");
        }
        if (req.page() <= 0 || req.limit() <= 0 || req.limit() > 10000) {
            throw new ExceptionMappers.InvalidRequest("invalid paging parameters");
        }
        Log.infof("got request: %s", req.toString());
        // List of CloudDumpPodsEntity
        var pods = podsService.getDumpPods(namespace, service, req.timeRange());
        // Convert to DTO
        return pods.stream().map(e -> DumpRecord.of(e, req.timeRange().from(), req.timeRange().to())).toList();
    }

    /**
     * Returns the list of hip dumps that is displayed on the Heap Dumps tab
     */
    @POST
    @Path("/heaps")
    public Collection<Responses.HeapDumpRecord> getHeapDumps(Requests.Services req) {
        // Validation
        if (!req.timeRange().isValid()) {
            throw new ExceptionMappers.InvalidRequest("invalid time range");
        }
        var res = dumpsService.getHeapDumps(req.timeRange(), req.services());
        // Convert to DTO
        return res.stream().map(Responses.HeapDumpRecord::of).toList();
    }

    Response prepareResponse(String fileExt, List<StreamRegistry> registries) {
        var result = dumpsService.prepareDownloadStream(fileExt, registries);
        return Response.ok(result.httpStream())
                .header(HttpHeaders.CONTENT_TYPE, "application/zip")
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=\"" + result.fileName() + "." + fileExt + ".zip\"")
                .build();
    }

    /**
     *
     */
    @DELETE
    @Path("/dumps/{podName}/{stream}/{seqId}")
    public Response deleteDump(@RestPath String podName, @RestPath String stream, @RestPath int seqId) {
        // validation
        var type = StreamType.byName(stream);
        if (type == null) {
            throw new ExceptionMappers.InvalidRequest("invalid stream type");
        }
        if (!PodIdRestart.isValid(podName)) {
            throw new ExceptionMappers.InvalidRequest("invalid pod id");
        }

        var podId = PodIdRestart.of(podName);
        var res = dumpsService.deleteStream(podId, type, seqId);
        if (!res) {
            throw new ExceptionMappers.UnknownData(String.format("no data for %s", type.getName()));
        }
        return Response.
                ok("DONE").
//                accepted("OK").
        type(MediaType.TEXT_PLAIN_TYPE).
                build();
    }


}
