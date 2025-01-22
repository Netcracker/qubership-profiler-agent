package org.qubership.profiler.cli;

import net.sourceforge.argparse4j.inf.Namespace;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ListServersTest {
    @Test
    public void listServers() {
        File testDirectory = new File("target", "test-classes");
        Assert.assertTrue(testDirectory.isDirectory(), "test directory could not be found (expected 'target/test-classes')");
        File dumpDirectory = new File(testDirectory, "execution-statistics-collector");
        Assert.assertTrue(dumpDirectory.isDirectory(), "test dump directory not found (expected target/test-classes/execution-statistics-collector')");

        Map<String, Object> map = new HashMap<>();
        map.put("dump_root", dumpDirectory.getAbsolutePath());
        Namespace ns = new Namespace(map);
        ListServers listServers = new ListServers();
        int res = listServers.accept(ns);
        Assert.assertEquals(res, 0, "listServers.accept should be successful");
    }
}
