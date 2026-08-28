package org.opensearch.client;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.StringUtils;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.*;

import java.util.Arrays;

public class RestHighLevelClient {

    public void internalPerformRequest$profiler(Object request, Object response) {
        if (!(request instanceof ActionRequest)) {
            return;
        }

        processRequest$profiler(request);

        if (response instanceof SearchResponse) {
            SearchResponse searchResponse = (SearchResponse) response;
            Profiler.event(searchResponse.getTook().getStringRep(), "os.response.took");
            Profiler.event(searchResponse.getHits().getHits().length, "os.response.hits");
            String scrollId = searchResponse.getScrollId();
            if (!StringUtils.isBlank(scrollId)) {
                Profiler.event(scrollId, "os.response.scrollId");
            }
            String pointInTimeId = searchResponse.pointInTimeId();
            if (!StringUtils.isBlank(pointInTimeId)) {
                Profiler.event(pointInTimeId, "os.response.pointInTimeId");
            }
            boolean isTimeout = searchResponse.isTimedOut();
            if (isTimeout) {
                Profiler.event(true, "os.response.isTimeout");
            }
        } else if (response instanceof IndexResponse) {
            IndexResponse indexResponse = (IndexResponse) response;
            Profiler.event(indexResponse.toString(), "os.response");
        } else if (response instanceof DeleteResponse) {
            DeleteResponse deleteResponse = (DeleteResponse) response;
            Profiler.event(deleteResponse.toString(), "os.response");
        } else if (response instanceof BulkResponse) {
            BulkResponse bulkResponse = (BulkResponse) response;
            Profiler.event(bulkResponse.getTook().getStringRep(), "os.response.took");
        } else if (response instanceof MultiSearchResponse) {
            MultiSearchResponse multiSearchResponse = (MultiSearchResponse) response;
            Profiler.event(multiSearchResponse.getTook().getStringRep(), "os.response.took");
        }
    }

    public void internalPerformRequestAsync$profiler(Object request) {

        if (!(request instanceof ActionRequest)) {
            return;
        }

        processRequest$profiler(request);
    }

    private void processRequest$profiler(Object request) {
        if (request instanceof SearchRequest) {
            Profiler.event("SearchRequest", "os.request.type");
            SearchRequest searchRequest = (SearchRequest) request;
            Profiler.event(Arrays.toString(searchRequest.indices()), "os.request.indices");
            Profiler.event(searchRequest.source().size(), "os.request.source.size");
            Profiler.event(searchRequest.source().query(), "os.request.source.query");
        } else if (request instanceof IndexRequest) {
            Profiler.event("IndexRequest", "os.request.type");
            IndexRequest indexRequest = (IndexRequest) request;
            Profiler.event(indexRequest.id(), "os.request.id");
            Profiler.event(indexRequest.getIndex$profiler(), "os.request.index");
        } else if (request instanceof DeleteRequest) {
            Profiler.event("DeleteRequest", "os.request.type");
            DeleteRequest deleteRequest = (DeleteRequest) request;
            Profiler.event(deleteRequest.id(), "os.request.id");
            Profiler.event(deleteRequest.getIndex$profiler(), "os.request.index");
        } else if (request instanceof BulkRequest) {
            Profiler.event("BulkRequest", "os.request.type");
            BulkRequest bulkRequest = (BulkRequest) request;
            Profiler.event(bulkRequest.getIndices().toString(), "os.request.indices");
            Profiler.event(bulkRequest.numberOfActions(), "os.request.count");
            Profiler.event(bulkRequest.estimatedSizeInBytes() + " bytes", "os.request.size");
        } else if (request instanceof SearchScrollRequest) {
            Profiler.event("SearchScrollRequest", "os.request.type");
            SearchScrollRequest searchScrollRequest = (SearchScrollRequest) request;
            Profiler.event(searchScrollRequest.scrollId(), "os.request.scrollId");
        } else if (request instanceof MultiSearchRequest) {
            Profiler.event("MultiSearchRequest", "os.request.type");
            MultiSearchRequest multiSearchRequest = (MultiSearchRequest) request;
            for (SearchRequest searchRequest : multiSearchRequest.requests()) {
                Profiler.event(Arrays.toString(searchRequest.indices()), "os.request.indices");
                Profiler.event(searchRequest.source().query(), "os.request.source.size");
                Profiler.event(searchRequest.source().query(), "os.request.source.query");
            }
        }
    }

}
