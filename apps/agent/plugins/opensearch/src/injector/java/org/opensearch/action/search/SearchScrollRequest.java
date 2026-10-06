package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;

public class SearchScrollRequest extends ActionRequest {
    public native String scrollId();
}
