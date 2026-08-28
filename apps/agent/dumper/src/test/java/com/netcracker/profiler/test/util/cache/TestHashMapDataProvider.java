package com.netcracker.profiler.test.util.cache;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class TestHashMapDataProvider {
    public static Stream<Arguments> createInstances() {
        List<Arguments> tests = new ArrayList<>();
        for (int j = 0; j < 5; j++)
            for (int i = 0; i < 10; i++)
                tests.add(arguments(50 + i + j * 10));
        return tests.stream();
    }
}
