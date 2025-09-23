package com.netcracker.profiler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.util.HashMap;
import java.util.Map;

public class DBResolver {
    public static final String CASSANDRA_HOST_ENV = "CASSANDRA_HOST";
    public static final String ELASTICSEARCH_HOST_ENV = "ES_HOST";
    private static final Logger log = LoggerFactory.getLogger(DBResolver.class);

    public Map<String, Object> properties;

    public DBResolver() {}

    public Map<String, Object> getProperties(SpringApplication app) {
        properties = new HashMap<String, Object>();
        if(System.getenv(CASSANDRA_HOST_ENV) != null) {
            log.info("Initializing cassandra storage profile");
            System.setProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration");
            System.setProperty("management.health.elasticsearch.enabled", "false");
            app.setAdditionalProfiles("cassandrastorage");
        } else if(System.getenv(ELASTICSEARCH_HOST_ENV) != null) {
            System.setProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration");
            System.setProperty("spring.codec.max-in-memory-size", "1MB");
            System.setProperty("management.health.cassandra.enabled", "false");
            log.info("Initializing elasticsearch storage profile");
            app.setAdditionalProfiles("elasticsearchstorage");
        } else {
            throw new RuntimeException("Unknown storage profile");
        }
        return properties;
    }

    public Map<String, Object> getUIProperties(SpringApplication app) {
        properties = new HashMap<String, Object>();
        if(System.getenv(CASSANDRA_HOST_ENV) != null) {
            log.info("Initializing cassandra storage profile");
            System.setProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration");
            app.setAdditionalProfiles("cassandrastorage");
        } else if(System.getenv(ELASTICSEARCH_HOST_ENV) != null) {
            System.setProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration");
            System.setProperty("spring.codec.max-in-memory-size", "1MB");
            log.info("Initializing elasticsearch storage profile");
            app.setAdditionalProfiles("elasticsearchstorage");
        } else {
            log.info("Initializing file storage profile");
            app.setAdditionalProfiles("filestorage");
            //no auto configuration of JMS, HTTP, cassandra or whatever is needed when in legacy mode
            properties.put("spring.boot.enableautoconfiguration", "false");
        }
        return properties;
    }
}
