package com.netcracker.profiler.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DiagtoolsFolderTest {
    @Test
    public void installFolderWinsOverRuntimeFolder() {
        assertEquals("/app/diag", DefaultCollectorClient.diagtoolsFolder(env("/app/diag", "/app/ncdiag")));
    }

    @Test
    public void runtimeFolderIsUsedWithoutInstallFolder() {
        // An older diag-bootstrap.sh exports only NC_DIAGNOSTIC_FOLDER
        assertEquals("/app/diag", DefaultCollectorClient.diagtoolsFolder(env(null, "/app/diag")));
    }

    @Test
    public void blankInstallFolderFallsBackToRuntimeFolder() {
        assertEquals("/app/diag", DefaultCollectorClient.diagtoolsFolder(env(" ", "/app/diag")));
    }

    @Test
    public void noFolderWhenBothAreBlank() {
        assertNull(DefaultCollectorClient.diagtoolsFolder(env("", null)));
    }

    private static Map<String, String> env(String home, String folder) {
        Map<String, String> env = new HashMap<>();
        if (home != null) {
            env.put("NC_DIAGNOSTIC_HOME", home);
        }
        if (folder != null) {
            env.put("NC_DIAGNOSTIC_FOLDER", folder);
        }
        return env;
    }
}
