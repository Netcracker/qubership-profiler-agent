package sun.net.www.http;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.agent.StringUtils;

import java.net.URL;

public class HttpClient {
    protected URL url;

    public void dumpHttpRequest$profiler() {
        String urlString = url == null ? null : url.toString();
        Profiler.event(StringUtils.truncateAndMark(urlString, ProfilerData.LOG_OUTGOING_REQUEST_TRIM_SIZE), "request");
    }
}
