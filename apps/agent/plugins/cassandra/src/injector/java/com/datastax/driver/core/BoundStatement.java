package com.datastax.driver.core;

public class BoundStatement extends Statement {
    public native PreparedStatement preparedStatement();
    public native <T> T get(int i, TypeCodec<T> codec);
}
