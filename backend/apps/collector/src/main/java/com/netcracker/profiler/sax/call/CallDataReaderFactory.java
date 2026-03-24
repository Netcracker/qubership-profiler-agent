package com.netcracker.profiler.sax.call;

public class CallDataReaderFactory {
    public static CallDataReader createReader(int fileFormat) {
        return switch (fileFormat) {
            case 1 -> new CallDataReader_01();
            case 2 -> new CallDataReader_02();
            case 3 -> new CallDataReader_03();
            case 4 -> new CallDataReader_04();
            default -> null;
        };
    }

}
