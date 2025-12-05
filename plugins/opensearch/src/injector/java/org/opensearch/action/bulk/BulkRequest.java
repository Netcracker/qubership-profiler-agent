package org.opensearch.action.bulk;

import org.opensearch.action.ActionRequest;

import java.util.Set;

public class BulkRequest extends ActionRequest {

    public native Set<String> getIndices();

    public native int numberOfActions();

    public native long estimatedSizeInBytes();

}
