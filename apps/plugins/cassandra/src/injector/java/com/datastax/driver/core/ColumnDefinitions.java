package com.datastax.driver.core;

import java.util.List;

public class ColumnDefinitions {
    public native int size();
    public native List<Definition> asList();
    public static class Definition {
        public native String getName();
        public native DataType getType();
    }
}
