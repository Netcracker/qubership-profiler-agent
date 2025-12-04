package org.opensearch.action.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHits;

public class SearchResponse {
    public native SearchHits getHits();

    public native TimeValue getTook();

    public native String getScrollId();

    public native String pointInTimeId();

    public native boolean isTimedOut();
}
