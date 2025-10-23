package com.netcracker.integration;

import com.netcracker.integration.cloud.PostgresResource;
import com.netcracker.integration.cloud.MinioResource;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Profiles {

    public static class CloudTest implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("service.persistence", "cloud");
        }

        @Override
        public String getConfigProfile() {
            return "cloud";
        }

        @Override
        public List<TestResourceEntry> testResources() {
            return List.of(new TestResourceEntry(MinioResource.class), new TestResourceEntry(PostgresResource.class));
        }

        @Override
        public Set<String> tags() {
            return Collections.singleton("CloudTest");
        }
    }
}
