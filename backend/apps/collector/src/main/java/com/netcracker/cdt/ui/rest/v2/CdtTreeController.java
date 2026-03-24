package com.netcracker.cdt.ui.rest.v2;

import com.netcracker.cdt.ui.services.tree.CallTreeMediator;
import com.netcracker.cdt.ui.services.tree.CallTreeRequest;
import com.netcracker.cdt.ui.services.tree.context.TraceRequestReader;
import com.netcracker.cdt.ui.services.tree.context.TreeDataLoader;
import com.netcracker.common.Time;
import com.netcracker.profiler.timeout.ProfilerTimeoutException;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
@Path("/cdt/v2")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CdtTreeController {
    @Inject
    Time time;
    @Inject
    TreeDataLoader treeDataLoader;

//    @Location("tree.html")
//    Template page;

    @GET
    @Path("/js/tree.js")
//    @RateLimited(bucket = "transfer")
    public Response callTree(HttpServerRequest req) {
        var context = CallTreeRequest.from(time.now(), false, req);
        if (context == null) {
            return Response.
                    status(Response.Status.BAD_REQUEST).
                    type(MediaType.APPLICATION_JSON_TYPE).
                    build();
        }
        try {
            var reader = new TraceRequestReader(treeDataLoader, context);
            var tree = reader.read();

            var mediator = new CallTreeMediator(context);
            var js = mediator.render(tree);

            return Response.
                    ok(js).
                    type("application/javascript").
                    build();
//            return page.data("js", js).render();
        } catch (ProfilerTimeoutException e) {
            Log.errorf(e, "");
            throw e;
        }
    }

//    var res = '<a target="_blank" href="tree.html#params-trim-size=15000
//    &f%5B_' + folderId + '%5D=' + encodeURIComponent(folderName) +
//    '&i=' + callHandle +
//    '&s=' + dataContext[C_TIME] +
//    '&e=' + (dataContext[C_TIME] + dataContext[C_DURATION])
//        /tree.html#params-trim-size=15000&f%5B_0%5D=" + encodeURL(rootReference.replace('\\', '/')) +
//            "&i=" + "0_" + call.traceFileIndex + "_" + call.bufferOffset + "_" + call.recordIndex + "_" + call.reactorFileIndex + "_" + call.reactorBufferOffset

}
