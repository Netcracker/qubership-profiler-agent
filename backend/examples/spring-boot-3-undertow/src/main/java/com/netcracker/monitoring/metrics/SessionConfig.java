package com.netcracker.monitoring.metrics;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.MapSession;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.web.http.SessionRepositoryFilter;

/**
 * Wires Spring Session with an in-memory repository so that every incoming HTTP request is
 * wrapped in {@code SessionRepositoryFilter$SessionRepositoryRequestWrapper}. This recreates
 * the production scenario that previously surfaced IllegalAccessException in the profiler's
 * HttpServletRequestAdapter when running against Undertow + Spring Session.
 *
 * The filter is constructed and registered explicitly so the behavior is independent of which
 * Spring Boot session auto-configuration is active for the current spring-session-core version.
 */
@Configuration
public class SessionConfig {

    @Bean
    public MapSessionRepository sessionRepository() {
        return new MapSessionRepository(new ConcurrentHashMap<>());
    }

    @Bean
    public FilterRegistrationBean<SessionRepositoryFilter<MapSession>> sessionFilterRegistration(
            MapSessionRepository repository) {
        SessionRepositoryFilter<MapSession> filter = new SessionRepositoryFilter<>(repository);
        FilterRegistrationBean<SessionRepositoryFilter<MapSession>> registration =
                new FilterRegistrationBean<>(filter);
        registration.addUrlPatterns("/*");
        registration.setOrder(Integer.MIN_VALUE + 50);
        return registration;
    }
}
