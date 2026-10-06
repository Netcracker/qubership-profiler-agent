package sun.net.www.http;

import com.netcracker.profiler.agent.Profiler;

import java.net.URL;

public class HttpClient {
    protected URL url;

    public void dumpHttpRequest$profiler() {
        Profiler.event(url, "request");
    }
}
