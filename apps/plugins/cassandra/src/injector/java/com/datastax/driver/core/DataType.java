package com.datastax.driver.core;

public class DataType {
    public String name;

    public native DataType.Name getName();
    public static class Name {
        public static Name CUSTOM,
        ASCII,
        BIGINT,
        BLOB,
        BOOLEAN,
        COUNTER,
        DECIMAL,
        DOUBLE,
        FLOAT,
        INT,
        TEXT,
        TIMESTAMP,
        UUID,
        VARCHAR,
        VARINT,
        TIMEUUID,
        INET,
        DATE,
        TIME,
        SMALLINT,
        TINYINT,
        DURATION,
        LIST,
        MAP,
        SET,
        UDT,
        TUPLE;
    }
}
