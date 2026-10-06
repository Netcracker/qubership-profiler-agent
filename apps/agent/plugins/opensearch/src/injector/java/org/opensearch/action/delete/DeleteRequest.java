package org.opensearch.action.delete;

import org.opensearch.action.ActionRequest;

public class DeleteRequest extends ActionRequest {

    String index;

    public native String id();

    public String getIndex$profiler() {
        return this.index;
    }

}
