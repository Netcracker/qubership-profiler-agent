package com.netcracker.profiler.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ExportDumpTest {
    static File getDumpRoot() {
        return uriToFile(ExportDumpTest.class.getResource("/execution-statistics-collector/dump/server1"))
                .getParentFile().getParentFile();
    }

    static File uriToFile(URL url) {
        if (!"file".equals(url.getProtocol())) {
            return null;
        }
        URI uri;
        try {
            uri = url.toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to convert URL " + url + " to URI", e);
        }
        if (uri.isOpaque()) {
            // It is like file:test%20file.c++
            // getSchemeSpecificPart would return "test file.c++"
            return new File(uri.getSchemeSpecificPart());
        }
        // See https://stackoverflow.com/a/17870390/1261287
        return Paths.get(uri).toFile();
    }

    @Test
    public void dumpWalk() {
        File dumpDirectory = getDumpRoot();
        File testDirectory = new File("build", "export-dump-test");
        testDirectory.mkdirs();
        File outputFile = new File(testDirectory, "test_export.zip");
        if (outputFile.isFile()) {
            assertTrue(outputFile.delete(), () -> "could not delete test export file - " + outputFile.getAbsolutePath());
        }

        Map<String, Object> map = new HashMap<>();
        map.put("start_date", "2022-02-02 20:21");
        map.put("end_date", "2022-02-02 20:22");
        map.put("time_zone", "Europe/Moscow");
        map.put("skip_details", false);
        map.put("dry_run", false);
        map.put("dump_root", dumpDirectory.getAbsolutePath());
        map.put("output_file", outputFile.getAbsolutePath());
        Namespace ns = new Namespace(map);
        ExportDump export = new ExportDump();
        int res = export.accept(ns);

        assertEquals(0, res, "export.accept should be successful");
        assertTrue(outputFile.isFile(), "output file must be generated");
        assertTrue(outputFile.length() > 0, "output file must have non-zero length");
        assertTrue(outputFile.delete(), () -> "could not delete test export file after test - " + outputFile.getAbsolutePath());
    }
}
