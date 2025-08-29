package org.springframework.web.client;

import org.qubership.profiler.agent.Profiler;

import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;

import java.net.URI;

public class RestTemplate {

    public void logRequestParameters$profiler(URI url, HttpMethod method, ClientHttpResponse response, Throwable t) {
        final String urlStr = url.toString();
        String address;
        String query;
        if (urlStr.contains("?")) {
            final String[] pair = urlStr.split("\\?");
            address = pair.length >=1? pair[0] : "";
            query = pair.length >= 2? pair[1]: "";
        }  else {
            address = urlStr;
            query = null;
        }

        Profiler.event(address, "resttemplate.url");
        Profiler.event(method, "resttemplate.method");
        if (query != null) {
            for (String param : query.split("&")) {
                final String[] paramAndValue = param.split("=");
                String paramName= paramAndValue.length >= 1 ? paramAndValue[0]: null;
                String paramValue= paramAndValue.length >= 2 ? paramAndValue[1]: "";
                if(paramName != null) {
                    Profiler.event(paramValue, "resttemplate." + paramName);
                }
            }

        }
    }
}
