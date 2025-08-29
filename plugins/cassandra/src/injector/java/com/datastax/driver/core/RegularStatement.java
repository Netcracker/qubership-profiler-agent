package com.datastax.driver.core;

public class RegularStatement extends Statement {
    public native String getQueryString();
}
