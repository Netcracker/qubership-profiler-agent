package com.netcracker.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcracker.utils.UnitTest;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UnitTest
public class PodStatTest {

    @Test
    public void testList() throws IOException, URISyntaxException {
        //read json file data to String
        URL a = PodStatTest.class.getResource("/pod/stat.bytes.json");
        Path b = Path.of(a.toURI());
        byte[] jsonData = Files.readAllBytes(b);

        ObjectMapper mapper = new ObjectMapper();
        List<Map<String,Object>> list = mapper.readValue(jsonData, List.class);
        assertEquals(1390, list.size());

        AtomicInteger s = new AtomicInteger(0);
        long sum = list.stream().map(e -> {
            return i(e.get("dataAtEnd")) - i(e.get("dataAtStart"));
        }).reduce(Long::sum).get();

        assertEquals(6285773664L, sum);
    }

    static long i(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        }
        if (o instanceof Integer) {
            return ((Integer) o).longValue();
        }
        if (o instanceof String) {
            return Long.parseLong((String) o);
        }
        return 0;
    }
}
