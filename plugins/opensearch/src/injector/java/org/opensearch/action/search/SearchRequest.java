package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.search.builder.SearchSourceBuilder;

public class SearchRequest extends ActionRequest {
    public native String[] indices();

    public native SearchSourceBuilder source();
}
