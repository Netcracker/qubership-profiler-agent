package com.netcracker.cdt.ui.rest.v2;

import com.netcracker.integration.Profiles;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BasicAuthProfile extends Profiles.CloudTest {
    public BasicAuthProfile() {
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        final Map<String, String> params = Map.of("quarkus.http.auth.basic", "true",
                "UI_USERNAME", "test_user",
                "UI_PASSWORD", "test_password");

        final HashMap<String, String> result = new HashMap<>();
        result.putAll(params);
        result.putAll(super.getConfigOverrides());

        return result;
    }

    @Override
    public Set<String> tags() {
        return Collections.singleton("CassandraWithBasicAuthTest");
    }
}
