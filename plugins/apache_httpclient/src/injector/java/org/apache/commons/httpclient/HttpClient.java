package org.apache.commons.httpclient;

import com.netcracker.profiler.agent.Profiler;

public class HttpClient {

    public void dumpHttpRequest$profiler(HttpMethod method) {
        if(method == null) return;
        try {
            URI uri = method.getURI();
            Profiler.event(uri, "http.request");
        } catch (URIException e) {
            return;
        }
    }

}
