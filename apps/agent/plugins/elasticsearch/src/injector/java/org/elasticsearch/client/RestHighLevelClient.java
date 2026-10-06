package org.elasticsearch.client;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.agent.TimerCache;

import org.elasticsearch.action.ActionRequest;

import java.io.IOException;

public class RestHighLevelClient {
    private static final ThreadLocal<String> currentRequestId$profiler = new ThreadLocal<>();
    private static final ThreadLocal<Integer> requestIdSequence$profiler = new ThreadLocal<>();

    //some elastic requests are chained. For example search request -> onResponse -> scroll request
    public static void setCurrentRequestId$profiler(String id) {
        currentRequestId$profiler.set(id);
    }

    public static String getCurrentRequestId$profiler() {
        try {
            String result = currentRequestId$profiler.get();

            currentRequestId$profiler.set(null);
            if (result == null) {
                return "unknown";
            }
            return result;
        } catch (Exception e) {
            //noop
        }
        return "unknown222";
    }

    public void internalPerformRequest$profiler(ActionRequest request, Response response) throws IOException {
        try {
            Profiler.enter("private <Req, Resp> org.elasticsearch.client.RestHighLevelClient.internalPerformRequest$profiler() (ElasticsearchRestTemplate.java:0) [unknown.jar]");

            //since elastic driver doesn't give us any information about timings and processess responses in a different thread
            //we need to pass reference to the calling thread and start time somehow
            Integer nextSeq = requestIdSequence$profiler.get();
            nextSeq = (nextSeq == null) ? 0 : nextSeq + 1;
            requestIdSequence$profiler.set(nextSeq);
            String req = request.toString();
            String resp = response.toString();

            String searchableInCallsList = ProfilerData.localState.get().callInfo.hashCode() + "_" + Thread.currentThread().getId();
            String currentRequestId = searchableInCallsList + "_" + TimerCache.now + "_" + nextSeq;
            Profiler.event(currentRequestId, "async.emitted");
            //this tag is not listed. only list the async response ones
            Profiler.event(req, "es.perform.req");
            Profiler.event(resp, "es.perform.resp");
            //pass query as well
            String logId = currentRequestId + "_" + req;
            setCurrentRequestId$profiler(logId);
        } catch (Exception e) {
            //noop
        } finally {
            Profiler.exit();
        }
    }

}
