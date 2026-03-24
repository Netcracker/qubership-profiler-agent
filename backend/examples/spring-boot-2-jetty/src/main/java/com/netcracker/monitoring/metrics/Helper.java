package com.netcracker.monitoring.metrics;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Helper { // framework-agnostic helper. MUST be same in all subprojects (just COPY-PASTE it)

    final State state;
    final Metrics metrics;

    public Helper(MeterRegistry registry, int listLimit) {
        this.state = new State(listLimit);
        this.metrics = new Metrics(registry);
    }

    class State { // keep current state ("server status", gauge, list, etc.)
        private String recentStatus = "UP";
        private AtomicInteger statusGauge = new AtomicInteger(0);

        private LinkedList<Long> list = new LinkedList<>();
        private final int listLimit; // max size of a linked list to prevent random OOM

        State(int listLimit) {
            this.listLimit = listLimit;
        }

        LinkedList<Long> getList() {
            return list;
        }

        Long updateList(long number) {
            if (number % 2 == 0) {
                if (list.size() > listLimit) {
                    return number;
                }
                list.add(number); // add even numbers to the list
            } else {
                number = removeFromList(); // remove items from the list for odd numbers
            }
            return number;
        }

        Long removeFromList() {
            try {
                return list.removeFirst();
            } catch (NoSuchElementException nse) {
                return 0L;
            }
        }
    }

    class Metrics { // list of micrometer metrics
        private final MeterRegistry registry;
        public final Counter countUp;
        public final Counter countDown;
        public final Gauge statusGauge;
        public final Timer timer;
        public final DistributionSummary summary;

        public Metrics(MeterRegistry registry) {
            this.registry = registry;

            registry.gaugeCollectionSize("example.list.size", Tags.empty(), state.getList());

            countUp = Counter
                    .builder("switch.state.up.total")
                    .description("Count of switch service state to \"UP\"")
                    .tag("code", "200")
                    .register(registry);
            countDown = Counter
                    .builder("switch.state.down.total")
                    .description("Count of switch service state to \"DOWN\"")
                    .tag("code", "200")
                    .register(registry);
            statusGauge = Gauge
                    .builder("service.status.gauge", state.statusGauge::doubleValue)
                    .description("Gauge of \"UP\" and \"DOWN\" statuses")
                    .tag("code", "200")
                    .register(registry);

            timer = Timer
                    .builder("my.timer")
                    .description("Request time execution for \"health\" endpoint")
                    .tags("code", "200")
                    .register(registry);
            summary = DistributionSummary
                    .builder("response.size")
                    .description("a description of what this summary does")
                    .tags("region", "test")
                    .scale(100)
                    .register(registry);
        }

        private Counter primeCounter(String tag) {
            return registry.counter("example.prime.number", "type", tag);
        }

        private Timer primeTimer() {
            return registry.timer("example.prime.number.test");
        }

    }

    // ---------------------------------------------------------------------------------------------------------------
    // endpoints

    public Map<String, String> getErrorResponse() {
        tooLoong();
        sleep();
        this.metrics.summary.record(100 * Math.random());
        return Collections.singletonMap("status", "Out of service");
    }

    public Map<String, String> getStatus() {
        tooLoong();
        sleep();
        this.metrics.timer.record((long) (100 * Math.random()), TimeUnit.MILLISECONDS);
        return Collections.singletonMap("status", this.state.recentStatus);
    }

    public Map<String, String> updateStatus(String status) {
        this.state.recentStatus = status == null ? "" : status.toUpperCase();

        if (this.state.recentStatus.equals("UP")) {
            this.metrics.countUp.increment();
            this.state.statusGauge.incrementAndGet();
        } else if (this.state.recentStatus.equals("DOWN")) {
            this.metrics.countDown.increment();
            this.state.statusGauge.decrementAndGet();
        }

        System.out.println("Status: " + this.state.recentStatus + ", gauge=" + this.state.statusGauge +
                ", `service.status.gauge` value: " + this.metrics.statusGauge.value());

        return Collections.singletonMap("status", this.state.recentStatus);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // test for prime number

    public String checkIfPrime(long number) {
        if (number < 1) {
            metrics.primeCounter("not-natural").increment();
            return "Only natural numbers can be prime numbers.";
        }
        if (number == 1) {
            metrics.primeCounter("one").increment();
            return number + " is not prime.";
        }
        if (number % 2 == 0) {
            metrics.primeCounter("even").increment();
            return number + " is not prime.";
        }

        if (testPrimeNumber(number)) {
            metrics.primeCounter("prime").increment();
            return number + " is prime.";
        } else {
            metrics.primeCounter("not-prime").increment();
            return number + " is not prime.";
        }
    }

    protected boolean testPrimeNumber(long number) {
        tooLoong();
        sleep();
        Timer timer = metrics.primeTimer();
        return timer.record(() -> isPrimeNumber(number));
    }

    boolean isPrimeNumber(long number) {
        for (int i = 3; i < Math.floor(Math.sqrt(number)) + 1; i = i + 2) {
            if (number % i == 0) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // random endpoint

    void emulateRandomEndpoint() {
        switch (new Random().nextInt(4)) {
            case 1:
                getErrorResponse();
                break;
            case 2:
                getStatus();
                break;
            case 3:
                updateStatus(Math.random() < 0.5 ? "UP" : "DOWN");
                break;
        }
    }

    void callRandomEndpoint(HttpClient client) {
        HttpRequest request = buildRequest();

        if (traceProbability()) {
            new Thread(() -> {
                sendRequest(client, request);
            }).start();
        } else {
            sendRequest(client, request);
        }
    }

    void sendRequest(HttpClient client, HttpRequest request) {
        HttpResponse<String> response = null;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Response: " + response);
        } catch (IOException | InterruptedException e) {
            System.out.println("Error: could not send random request: " + response);
            throw new RuntimeException(e);
        }
    }

    HttpRequest buildRequest() {
        String url = randomUrl();
        if (url == null) {
            System.out.println("Could not generate random url");
            return null;
        }

        var requestBuilder = HttpRequest.newBuilder(URI.create(url));
        if (traceProbability()) {
            requestBuilder.header("X-B3-TraceId", randomTraceId());
        }
        if (traceProbability()) {
            requestBuilder.header("X-B3-SpanId", randomTraceId());
        }
        if (traceProbability()) {
            requestBuilder.header("X-B3-ParentSpanId", randomTraceId());
        }
        if (traceProbability()) {
            requestBuilder.header("x-client-transaction-id", randomTransactionId());
        }
        if (traceProbability()) {
            requestBuilder.header("x-request-id", randomRequestId());
        }
        return requestBuilder.build();
    }

    String randomUrl() {
        switch (new Random().nextInt(1, 5)) {
            case 1: return "http://localhost:8080/custom/err";
            case 2: return "http://localhost:8080/custom/health";
            case 3: {
                String status = Math.random() < 0.5 ? "UP" : "DOWN";
                return "http://localhost:8080/custom/health?status=" + status;
            }
            case 4: {
                int number = new Random().nextInt(10000);
                return "http://localhost:8080/custom/prime/" + number;
            }
        };
        return null;
    }

    String randomRequestId() {
        return traceProbability() ? "f26b33a326b86fe3" : "96a101dd-c49a-4fea-aee2-a76510f32190";
    }

    String randomTransactionId() {
        return traceProbability() ? "f26b33a326b86fe3" : "102656693ac3ca6e0cdafbfe89ab99";
    }

    String randomTraceId() {
        return traceProbability() ? "f26b33a326b86fe3" : "463ac35c9f6413ad48485a3953bb6124";
    }

    boolean traceProbability() {
        return Math.random() < 0.1;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // utils

    void tooLoong() {
        tooLoong(5000, random()); // 5s timeout
    }

    void tooLoong(int timeoutMs, int n) {
        long stopTime = System.currentTimeMillis() + timeoutMs;
        double max = 0;
        for (int i = 0; i < n; i++) {
            double dist = calcRandomDistance();
            if (dist > max) {
                max = dist;
            }
            if (System.currentTimeMillis() > stopTime) {
                break;
            }
        }
        System.out.println("Max value is: " + max);
    }

    void outOfMemory(int megabytes) {
        long size = 0;
        List<String> list = new ArrayList<>(megabytes);
        for (int i = 0; i < megabytes; i++) {
            String s = generateString();
            list.add(s);
            size += s.length();
            sleep(50); // 20mb/s
        }
        System.out.println("Generated "+megabytes+" strings, total size " + (size/1024/1024) + " Mb");
    }

    String generateString() {
        StringBuffer a = new StringBuffer(1000000);
        for (int i= 0; i<=100000; i++) {
            a.append("1234567890");
        }
        return a.toString();
    }

    double calcRandomDistance() {
        double x1 = Math.random();
        double y1 = Math.random();
        double x2 = Math.random();
        double y2 = Math.random();
        double dx = x1 - x2;
        double dy = y1 - y2;
        double dist = Math.sqrt(dx * dx + dy * dy);
        return dist;
    }

    int random() {
        int n = ThreadLocalRandom.current().nextInt(5_000_000, 10_000_000);
        return n;
    }

    void sleep() {
        sleep((int) Math.round(500 * Math.random()));
    }

    void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
