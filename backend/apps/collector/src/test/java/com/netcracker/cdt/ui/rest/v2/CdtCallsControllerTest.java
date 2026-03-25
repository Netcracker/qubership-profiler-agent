package com.netcracker.cdt.ui.rest.v2;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.PodEmulator;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.api.v2.*;
import com.netcracker.fixtures.data.PodBinaryData;
import jakarta.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(SAME_THREAD)
public abstract class CdtCallsControllerTest {
    public static final PodName POD1 = TestHelper.pod("a", "a", 1);

    public static final Instant CT1 = Instant.parse("2023-07-24T05:20:27.000Z"); // for calls
    public static final Instant CT2 = Instant.parse("2023-07-25T07:47:27.000Z");

    public static final PodBinaryData DATA = PodBinaryData.TEST_SERVICE;

    @Inject
    TestHelper test;
    static PodEmulator pod1;

    static AtomicBoolean init = new AtomicBoolean(false);

    @BeforeEach
    public void start() throws Exception {
        if (init.compareAndSet(false, true)) { // upload only once per suite
            try (var ignored = test.withTime(CT1)) {
                pod1 = test.startPod(CT1, POD1, DATA);
                pod1.sendStream(StreamType.CALLS);
                pod1.finish();
                pod1.persistStat();
            }
        }
    }

    @Nested
    @DisplayName("/v2/calls")
    class CallsListController {

        @Nested
        @DisplayName("when searching for calls")
        @Execution(SAME_THREAD)
        class CallsTests {
            final Calls api = new Calls();

            @Test
            @DisplayName("should return all calls in time range")
            public void testCallsInTimeRange() {
                test.setTime(CT1);
                var res = api.retrieveData(api.POST(200,
                        api.postSearchRequest("1", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "")
                ));
                res.assertCalls(6);
                res.assertJson("""
                        {
                        "status":{"finished":true,"progress":100,"errorMessage":"","filteredRecords":6,"processedRecords":6},
                        "calls":[
                            {"ts":1690201583767,"duration":1387,"cpuTime":1459,"suspend":0,"queue":0,"calls":4,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profiler-runtime.jar]",
                                "traceId":"1_8_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["main"]}},
                            {"ts":1690201708540,"duration":1646,"cpuTime":38,"suspend":0,"queue":0,"calls":7,"transactions":0,"diskBytes":0,"netBytes":2110245,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_8182_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["a42210d20bc079a9efc5319fd6207218"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-2"],
                                    "profiler.title":["GET /static/js/2.31eb250d.chunk.js, client: 10.236.118.197"],
                                    "_web.referer":["https://esc-ui-service.test.org/"],
                                    "web.url":["https://esc-ui-service.test.org/static/js/2.31eb250d.chunk.js"]}},
                            {"ts":1690201711779,"duration":1564,"cpuTime":182,"suspend":0,"queue":0,"calls":18,"transactions":0,"diskBytes":436,"netBytes":2297,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_14844_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["3ae715cc041a776867024646d0edf50e"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-7"],
                                    "profiler.title":["GET /esc/calls/load?windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&, client: 10.236.118.197"],
                                    "web.query":["windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&"],
                                    "_web.referer":["https://esc-ui-service.test.org/esc/calls?filterParams=%7B%22dateRange%22%3A%5B%222023-07-24T12%3A13%3A31.480Z%22%2C%222023-07-24T12%3A28%3A31.481Z%22%5D%2C%22dateRangeOption%22%3A%22LAST_15_MIN%22%2C%22duration%22%3A%22%3E%3D%205000ms%22%2C%22podsToLoad%22%3A%5B%5D%2C%22filterString%22%3A%22%22%2C%22hideSystem%22%3Atrue%7D"],
                                    "web.url":["https://esc-ui-service.test.org/esc/calls/load"]}},
                            {"ts":1690201723745,"duration":1414,"cpuTime":15,"suspend":0,"queue":0,"calls":11,"transactions":0,"diskBytes":391,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_40128_0","pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-3-thread-1"]}},
                            {"ts":1690201724046,"duration":1197,"cpuTime":126,"suspend":0,"queue":0,"calls":66,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_38086_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-4-thread-1"]}},
                            {"ts":1690201725144,"duration":1109,"cpuTime":98,"suspend":0,"queue":0,"calls":271,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_42505_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-4-thread-4"]}}
                        ]}
                        """);
            }

            @Ignore
            @Test
            @DisplayName("should return calls by query")
            public void testCallsWithQueryInTimeRange() {
                test.setTime(CT1);

                var res = api.retrieveData(api.POST(200,
                        api.postSearchRequest("1", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "+$x-request-id=a42210d20bc079a9efc5319fd6207218")
                ));
                res.assertCalls(1);
                res.assertJson("""
                        {"status":{"finished":true,"progress":100,"errorMessage":"","filteredRecords":1,"processedRecords":6},
                         "calls":[
                            {"ts":1690201708540,"duration":1646,"cpuTime":38,"suspend":0,"queue":0,"calls":7,"transactions":0,"diskBytes":0,"netBytes":2110245,"memoryUsed":0,
                            "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                            "traceId":"1_8182_0","pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                            "params":{"x-request-id":["a42210d20bc079a9efc5319fd6207218"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],
                                "java.thread":["http-nio-8180-exec-2"],"profiler.title":["GET /static/js/2.31eb250d.chunk.js, client: 10.236.118.197"],
                                "_web.referer":["https://esc-ui-service.test.org/"],
                                "web.url":["https://esc-ui-service.test.org/static/js/2.31eb250d.chunk.js"]}}
                        ]}
                """);

                res = api.retrieveData(api.POST(200,
                        api.postSearchRequest("1", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "web.remote.addr")
                ));
                res.assertCalls(2);
                res.assertJson("""
                        {"status":{"finished":true,"progress":100,"errorMessage":"","filteredRecords":2,"processedRecords":6},
                         "calls":[
                            {"ts":1690201708540,"duration":1646,"cpuTime":38,"suspend":0,"queue":0,"calls":7,"transactions":0,"diskBytes":0,"netBytes":2110245,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_8182_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["a42210d20bc079a9efc5319fd6207218"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-2"],
                                    "profiler.title":["GET /static/js/2.31eb250d.chunk.js, client: 10.236.118.197"],
                                    "_web.referer":["https://esc-ui-service.test.org/"],
                                    "web.url":["https://esc-ui-service.test.org/static/js/2.31eb250d.chunk.js"]}},
                            {"ts":1690201711779,"duration":1564,"cpuTime":182,"suspend":0,"queue":0,"calls":18,"transactions":0,"diskBytes":436,"netBytes":2297,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_14844_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["3ae715cc041a776867024646d0edf50e"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-7"],
                                    "profiler.title":["GET /esc/calls/load?windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&, client: 10.236.118.197"],
                                    "web.query":["windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&"],
                                    "_web.referer":["https://esc-ui-service.test.org/esc/calls?filterParams=%7B%22dateRange%22%3A%5B%222023-07-24T12%3A13%3A31.480Z%22%2C%222023-07-24T12%3A28%3A31.481Z%22%5D%2C%22dateRangeOption%22%3A%22LAST_15_MIN%22%2C%22duration%22%3A%22%3E%3D%205000ms%22%2C%22podsToLoad%22%3A%5B%5D%2C%22filterString%22%3A%22%22%2C%22hideSystem%22%3Atrue%7D"],
                                    "web.url":["https://esc-ui-service.test.org/esc/calls/load"]}}
                        ]}
                """);

                res = api.retrieveData(api.POST(200,
                        api.postSearchRequest("1", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "\"x-request-id\"")
                ));
                res.assertCalls(2);
                res.assertJson("""
                        {"status":{"finished":true,"progress":100,"errorMessage":"","filteredRecords":2,"processedRecords":6},
                         "calls":[
                            {"ts":1690201708540,"duration":1646,"cpuTime":38,"suspend":0,"queue":0,"calls":7,"transactions":0,"diskBytes":0,"netBytes":2110245,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_8182_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["a42210d20bc079a9efc5319fd6207218"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-2"],
                                    "profiler.title":["GET /static/js/2.31eb250d.chunk.js, client: 10.236.118.197"],
                                    "_web.referer":["https://esc-ui-service.test.org/"],
                                    "web.url":["https://esc-ui-service.test.org/static/js/2.31eb250d.chunk.js"]}},
                            {"ts":1690201711779,"duration":1564,"cpuTime":182,"suspend":0,"queue":0,"calls":18,"transactions":0,"diskBytes":436,"netBytes":2297,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_14844_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["3ae715cc041a776867024646d0edf50e"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-7"],
                                    "profiler.title":["GET /esc/calls/load?windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&, client: 10.236.118.197"],
                                    "web.query":["windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&"],
                                    "_web.referer":["https://esc-ui-service.test.org/esc/calls?filterParams=%7B%22dateRange%22%3A%5B%222023-07-24T12%3A13%3A31.480Z%22%2C%222023-07-24T12%3A28%3A31.481Z%22%5D%2C%22dateRangeOption%22%3A%22LAST_15_MIN%22%2C%22duration%22%3A%22%3E%3D%205000ms%22%2C%22podsToLoad%22%3A%5B%5D%2C%22filterString%22%3A%22%22%2C%22hideSystem%22%3Atrue%7D"],
                                    "web.url":["https://esc-ui-service.test.org/esc/calls/load"]}}
                        ]}
                """);

                res = api.retrieveData(api.POST(200,
                        api.postSearchRequest("1", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "LongPriorityFutureTask")
                ));
                res.assertCalls(3);
                res.assertJson("""
                        {"status":{"finished":true,"progress":100,"errorMessage":"","filteredRecords":3,"processedRecords":6},
                         "calls":[
                             {"ts":1690201723745,"duration":1414,"cpuTime":15,"suspend":0,"queue":0,"calls":11,"transactions":0,"diskBytes":391,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_40128_0","pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-3-thread-1"]}},
                            {"ts":1690201724046,"duration":1197,"cpuTime":126,"suspend":0,"queue":0,"calls":66,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_38086_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-4-thread-1"]}},
                            {"ts":1690201725144,"duration":1109,"cpuTime":98,"suspend":0,"queue":0,"calls":271,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_42505_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-4-thread-4"]}}
                        ]}
                """);
            }

            @Test
            @DisplayName("should return calls by sorting")
            public void testSortCalls() {
                test.setTime(CT1);
                var res = api.retrieveData(api.POST(200,
                        api.postSortedSearchRequest("1", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "duration", false)
                ));
                res.assertCalls(6);
                res.assertJson("""
                        {
                        "status":{"finished":true,"progress":100,"errorMessage":"","filteredRecords":6,"processedRecords":6},
                        "calls":[
                            {"ts":1690201708540,"duration":1646,"cpuTime":38,"suspend":0,"queue":0,"calls":7,"transactions":0,"diskBytes":0,"netBytes":2110245,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_8182_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["a42210d20bc079a9efc5319fd6207218"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-2"],
                                    "profiler.title":["GET /static/js/2.31eb250d.chunk.js, client: 10.236.118.197"],
                                    "_web.referer":["https://esc-ui-service.test.org/"],
                                    "web.url":["https://esc-ui-service.test.org/static/js/2.31eb250d.chunk.js"]}},
                            {"ts":1690201711779,"duration":1564,"cpuTime":182,"suspend":0,"queue":0,"calls":18,"transactions":0,"diskBytes":436,"netBytes":2297,"memoryUsed":0,
                                "title":"void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]",
                                "traceId":"1_14844_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},
                                "params":{
                                    "x-request-id":["3ae715cc041a776867024646d0edf50e"],"web.method":["GET"],"web.remote.addr":["10.236.118.197"],"java.thread":["http-nio-8180-exec-7"],
                                    "profiler.title":["GET /esc/calls/load?windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&, client: 10.236.118.197"],
                                    "web.query":["windowId=1690201711480_152211&clientUTC=1690201711602&timerangeFrom=1690200811480&timerangeTo=1690201711481&durationFrom=5000&durationTo=30879000&podFilter=%7B%22operation%22%3A%22or%22%2C%22conditions%22%3A%5B%5D%7D&filterString=&beginIndex=0&pageSize=100&hideSystem=true&sortIndex=0&asc=false&"],
                                    "_web.referer":["https://esc-ui-service.test.org/esc/calls?filterParams=%7B%22dateRange%22%3A%5B%222023-07-24T12%3A13%3A31.480Z%22%2C%222023-07-24T12%3A28%3A31.481Z%22%5D%2C%22dateRangeOption%22%3A%22LAST_15_MIN%22%2C%22duration%22%3A%22%3E%3D%205000ms%22%2C%22podsToLoad%22%3A%5B%5D%2C%22filterString%22%3A%22%22%2C%22hideSystem%22%3Atrue%7D"],
                                    "web.url":["https://esc-ui-service.test.org/esc/calls/load"]}},
                            {"ts":1690201723745,"duration":1414,"cpuTime":15,"suspend":0,"queue":0,"calls":11,"transactions":0,"diskBytes":391,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_40128_0","pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-3-thread-1"]}},
                            {"ts":1690201583767,"duration":1387,"cpuTime":1459,"suspend":0,"queue":0,"calls":4,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profiler-runtime.jar]",
                                "traceId":"1_8_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["main"]}},
                            {"ts":1690201724046,"duration":1197,"cpuTime":126,"suspend":0,"queue":0,"calls":66,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_38086_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-4-thread-1"]}},
                            {"ts":1690201725144,"duration":1109,"cpuTime":98,"suspend":0,"queue":0,"calls":271,"transactions":0,"diskBytes":0,"netBytes":0,"memoryUsed":0,
                                "title":"void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar]",
                                "traceId":"1_42505_0",
                                "pod":{"namespace":"aCdtCallsControllerTest","service":"aCdtCallsControllerTest","pod":"aCdtCallsControllerTest-CdtCallsControllerTest-1","startTime":1690176027000},"params":{"java.thread":["pool-4-thread-4"]}}
                        ]}
                        """);
            }


        }
    }


    @Nested
    @DisplayName("/v2/export")
    class CallsExportController {

        @Nested
        @DisplayName("when exporting for calls")
        @Execution(SAME_THREAD)
        class CallsTests {
            final Calls api = new Calls();

            @Test
            @DisplayName("should export all calls in time range")
            public void testExportCsvInTimeRange() {
                var S1 = new Requests.Service("aCdtCallsControllerTest", "aCdtCallsControllerTest");
                test.setTime(CT1);
                var res = api.retrieveZip(api.GET(200,
                        api.exportRequest("csv", TimeRange.of(CT1, CT2), DurationRange.ofSeconds(1, 2), "", S1)
                ));

                res.assertFile("20230724T052027UTC.csv.zip", """
Start timestamp ; Duration ; CPU Time(ms) ; Suspended(ms) ; Queue(ms) ; Calls ; Transactions ; Disk Read (B) ; Disk Written (B) ; RAM (B) ; Logs generated ; Logs written (B) ; Net read (B) ; Net written (B) ; Namespace ; Service Name ; POD ; method
2023-07-24 12:26:23 ; 1387 ; 1459 ; 0 ; 0 ; 4 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profiler-runtime.jar] ;
2023-07-24 12:26:25 ; 93906 ; 13221 ; 0 ; 0 ; 677 ; 0 ; 643899 ; 6935 ; 0 ; 0 ; 0 ; 0 ; 0 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void com.netcracker.cdt.uiservice.UiServiceApplication.main(java.lang.String[]) (UiServiceApplication.java:58) [escui.jar!/BOOT-INF/classes] ;
2023-07-24 12:28:01 ; 2043 ; 321 ; 0 ; 0 ; 6 ; 0 ; 0 ; 429 ; 0 ; 0 ; 0 ; 121 ; 759 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar] ;
2023-07-24 12:28:01 ; 2047 ; 148 ; 0 ; 0 ; 3 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 121 ; 759 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar] ;
2023-07-24 12:28:28 ; 1646 ; 38 ; 0 ; 0 ; 7 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 1150 ; 2109095 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar] ;
2023-07-24 12:28:31 ; 1564 ; 182 ; 0 ; 0 ; 18 ; 0 ; 0 ; 436 ; 0 ; 0 ; 0 ; 1750 ; 547 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar] ;
2023-07-24 12:28:43 ; 3236 ; 122 ; 0 ; 0 ; 23 ; 0 ; 0 ; 812 ; 0 ; 0 ; 0 ; 2188 ; 19244 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar] ;
2023-07-24 12:28:44 ; 1197 ; 126 ; 0 ; 0 ; 66 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar] ;
2023-07-24 12:28:43 ; 1414 ; 15 ; 0 ; 0 ; 11 ; 0 ; 0 ; 391 ; 0 ; 0 ; 0 ; 0 ; 0 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar] ;
2023-07-24 12:28:43 ; 2509 ; 78 ; 0 ; 0 ; 18 ; 0 ; 0 ; 391 ; 0 ; 0 ; 0 ; 0 ; 0 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar] ;
2023-07-24 12:28:45 ; 1109 ; 98 ; 0 ; 0 ; 271 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; 0 ; aCdtCallsControllerTest ; aCdtCallsControllerTest ; aCdtCallsControllerTest-CdtCallsControllerTest-1 ; void com.netcracker.profiler.storage.parserscassandra.LongPriorityFutureTask.run() (LongPriorityFutureTask.java:67) [BOOT-INF/lib/parsers-cassandra-9.3.2.64.jar] ;
""");
            }



        }
    }


}
