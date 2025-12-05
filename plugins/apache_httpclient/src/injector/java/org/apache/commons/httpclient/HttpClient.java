package org.apache.commons.httpclient;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.agent.StringUtils;

public class HttpClient {

    public void dumpHttpRequest$profiler(HttpMethod method) {
        if(method == null) return;
        try {
            URI uri = method.getURI();
            String urlString = uri == null ? null : uri.toString();
            urlString = StringUtils.truncateAndMark(urlString, ProfilerData.LOG_OUTGOING_REQUEST_TRIM_SIZE);
            Profiler.event(urlString, "http.request");
        } catch (URIException e) {
            return;
        }
    }

}
