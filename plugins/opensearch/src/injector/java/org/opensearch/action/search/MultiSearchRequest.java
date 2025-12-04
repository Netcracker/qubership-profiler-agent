package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;

import java.util.List;

public class MultiSearchRequest extends ActionRequest {
    public native List<SearchRequest> requests();
}
