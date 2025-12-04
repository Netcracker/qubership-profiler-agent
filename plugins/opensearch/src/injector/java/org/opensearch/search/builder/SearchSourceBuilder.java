package org.opensearch.search.builder;

import org.opensearch.index.query.QueryBuilder;

public class SearchSourceBuilder {
    public native QueryBuilder query();

    public native int size();
}
