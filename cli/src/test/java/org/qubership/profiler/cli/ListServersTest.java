package org.qubership.profiler.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ListServersTest {
    @Test
    public void listServers() {
        File dumpDirectory = ExportDumpTest.getDumpRoot();

        Map<String, Object> map = new HashMap<>();
        map.put("dump_root", dumpDirectory.getAbsolutePath());
        Namespace ns = new Namespace(map);
        ListServers listServers = new ListServers();
        int res = listServers.accept(ns);
        assertEquals(0, res, "listServers.accept should be successful");
    }
}
