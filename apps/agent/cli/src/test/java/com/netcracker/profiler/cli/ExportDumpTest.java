package com.netcracker.profiler.cli;

import static com.netcracker.profiler.testkit.resources.URLExtensions.getResourceAsFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class ExportDumpTest {
    static File getDumpRoot() throws FileNotFoundException {
        return getResourceAsFile(ExportDumpTest.class, "/execution-statistics-collector/dump/server1")
                .getParentFile().getParentFile();
    }

    @Test
    public void dumpWalk() throws FileNotFoundException {
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
        ExportDump export = new ExportDump(dumpDirectory);
        int res = export.accept(ns);

        assertEquals(0, res, "export.accept should be successful");
        assertTrue(outputFile.isFile(), "output file must be generated");
        assertTrue(outputFile.length() > 0, "output file must have non-zero length");
        assertTrue(outputFile.delete(), () -> "could not delete test export file after test - " + outputFile.getAbsolutePath());
    }
}
