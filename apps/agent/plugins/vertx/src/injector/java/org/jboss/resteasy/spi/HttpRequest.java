package org.jboss.resteasy.spi;

import jakarta.ws.rs.core.HttpHeaders;

public interface HttpRequest {
    HttpHeaders getHttpHeaders();
}
