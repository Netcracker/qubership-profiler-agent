package org.apache.http.protocol;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.agent.StringUtils;

import org.apache.http.HttpRequest;
import org.apache.http.RequestLine;

public class HttpRequestExecutor {

    public void dumpHttpRequest$profiler(HttpRequest req) {
        if(req == null) return;
        RequestLine requestLine = req.getRequestLine();
        if(requestLine == null) return;
        String url = StringUtils.truncateAndMark(requestLine.getUri(), ProfilerData.LOG_OUTGOING_REQUEST_TRIM_SIZE);
        Profiler.event(url, "http.request");
    }

}
