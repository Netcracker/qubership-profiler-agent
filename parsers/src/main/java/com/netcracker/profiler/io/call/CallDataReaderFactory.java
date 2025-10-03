package com.netcracker.profiler.io.call;

public class CallDataReaderFactory {
    public static CallDataReader createReader(int fileFormat) {
        switch (fileFormat) {
            case 1:
                return new CallDataReader_01();
            case 2:
                return new CallDataReader_02();
            case 3:
                return new CallDataReader_03();
            case 4:
                return new CallDataReader_04();
            default:
                return new CallDataReader_00();
        }
    }
}
