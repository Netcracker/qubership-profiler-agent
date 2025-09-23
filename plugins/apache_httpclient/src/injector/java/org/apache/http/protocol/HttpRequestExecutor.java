package org.apache.http.protocol;

import com.netcracker.profiler.agent.Profiler;

import org.apache.http.HttpRequest;
import org.apache.http.RequestLine;

public class HttpRequestExecutor {

    public void dumpHttpRequest$profiler(HttpRequest req) {
        if(req == null) return;
        RequestLine requestLine = req.getRequestLine();
        if(requestLine == null) return;
        Profiler.event(requestLine.getUri(), "http.request");
    }

}
