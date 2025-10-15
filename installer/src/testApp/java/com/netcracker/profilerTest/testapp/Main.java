package com.netcracker.profilerTest.testapp;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello World!");
        System.out.println("Waiting for profiler agent to initialize and send data...");
        // Give profiler agent time to initialize and connect to collector
        Thread.sleep(5000);
        System.out.println("Test application completed.");
    }
}
