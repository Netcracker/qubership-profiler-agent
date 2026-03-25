package com.netcracker.cdt.ui.rest.v2;

import com.netcracker.fixtures.api.v2.*;
import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.PodEmulator;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.data.PodBinaryData;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(SAME_THREAD)
public abstract class CdtControllerTest {

    @ConfigProperty(name = "cdt.version", defaultValue = "1.0.0")
    public String expectedCdtVersion;

    public static final PodName POD1 = TestHelper.pod("a", "a", 1);
    public static final PodName POD2 = TestHelper.pod("a", "a", 2);
    public static final PodName POD3 = TestHelper.pod("a", "b", 3);
    public static final PodName POD4 = TestHelper.pod("b", "b", 4);

    public static final Instant T1 = Instant.parse("2021-06-28T02:19:27.000Z"); // not to overlap with other tests
    public static final Instant T2 = Instant.parse("2021-06-28T02:22:27.000Z");
    public static final Instant T3 = Instant.parse("2021-06-28T02:41:27.000Z");
    public static final Instant T4 = Instant.parse("2021-06-28T02:47:27.000Z");

    public static final PodBinaryData DATA = PodBinaryData.SMALL_SERVICE;

    @Inject
    TestHelper test;
    static PodEmulator pod1;
    static PodEmulator pod2;
    static PodEmulator pod3;
    static PodEmulator pod4;

    static AtomicBoolean init = new AtomicBoolean(false);

    @BeforeEach
    public void start() throws Exception {
        if (init.compareAndSet(false, true)) { // upload only once per suite
            pod1 = test.startPod(T1, POD1, DATA);
            pod2 = test.startPod(T2, POD2, DATA);
            pod3 = test.startPod(T3, POD3, DATA);
            pod4 = test.startPod(T4, POD4, DATA);
        }
    }


    @Nested
    @DisplayName("/v2/cdt-controller")
    class CdtController {
        @Nested
        @DisplayName("when asked for version")
        class VersionTests {
            final Containers api = new Containers();

            @Test
            @DisplayName("should return current deployment")
            public void testVersion() {
                var version = api.GetVersion(200);
                assertEquals(expectedCdtVersion, version);
            }

        }

        @Nested
        @DisplayName("when asked for services")
        class NamespacesTests {
            final Containers api = new Containers();

            @Test
            @DisplayName("should return known services")
            public void testNamespaces() {

                var res = api.retrieveData(api.GetContainers());
                res.assertSize(2);
                res.assertJson("""
                    [ {
                        "namespace":"aCdtControllerTest",
                        "services":[
                            {"name":"aCdtControllerTest", "lastAck":1624846947000, "activePods":2},
                            {"name":"bCdtControllerTest", "lastAck":1624848087000, "activePods":1}
                        ]
                    }, {
                        "namespace":"bCdtControllerTest",
                        "services":[
                            {"name":"bCdtControllerTest", "lastAck":1624848447000, "activePods":1}
                        ]
                    } ]""");
            }

            @Test
            @DisplayName("should return services meet the filter criteria")
            public void testSearchNamespaces() {
                var res = api.retrieveData(
                        api.PostContainers(200, api.postContainersRequest(TimeRange.of(T1, T4), 10, 1))
                );
                res.assertSize(2);
                res.assertJson("""
                    [ {
                        "namespace":"aCdtControllerTest",
                        "services":[
                            {"name":"aCdtControllerTest",  "lastAck":1624846947000, "activePods":2},
                            {"name":"bCdtControllerTest", "lastAck":1624848087000, "activePods":1}
                        ]
                    }, {
                        "namespace":"bCdtControllerTest",
                        "services":[
                            {"name":"bCdtControllerTest", "lastAck":1624848447000, "activePods":1}
                        ]
                    } ]""");
            }

            @Test
            @DisplayName("should not be errors during json unmarshalling")
            public void testSearchNamespacesJson() {
                var res = api.retrieveData(
                        api.PostContainers(200, api.postContainersRequest(TimeRange.of(T1, T4), 10, 1))
                );
                res.assertSize(2);
                res.assertJson("""
                    [ {
                        "namespace":"aCdtControllerTest",
                        "services":[
                            {"name":"aCdtControllerTest",  "lastAck":1624846947000, "activePods":2},
                            {"name":"bCdtControllerTest", "lastAck":1624848087000, "activePods":1}
                        ]
                    }, {
                        "namespace":"bCdtControllerTest",
                        "services":[
                            {"name":"bCdtControllerTest", "lastAck":1624848447000, "activePods":1}
                        ]
                    } ]""");
            }

            @Test
            @DisplayName("should validate time range in filter")
            public void testIncorrectTimeRange() {
                api.PostContainers(400, api.postContainersRequest(TimeRange.of(T4, T1), 10, 1));
            }

            @Test
            @DisplayName("should validate paging parameters")
            public void testIncorrectPaging() {
                api.PostContainers(400, api.postContainersRequest(TimeRange.of(T1, T4), -1, 1));
                api.PostContainers(400, api.postContainersRequest(TimeRange.of(T1, T4), 0, 1));
                api.PostContainers(400, api.postContainersRequest(TimeRange.of(T1, T4), 5, -1));
                api.PostContainers(400, api.postContainersRequest(TimeRange.of(T1, T4), 5, 0));
            }


        }

    }


     @Nested
    @DisplayName("/v2/pods-info-controller")
    class PodsController {

        final Pods api = new Pods();

        @Nested
        @DisplayName("when searching for pods")
        class ServicePodsTests {

            @Test
            @DisplayName("should return pods in time range")
            public void testAllActivePods() {
                var res = api.retrieveData(
                        api.POST(200, api.postServiceRequest(TimeRange.of(T1, T4), "aaa"))
                );
                res.assertPods(4);
            }

            @Test
            @DisplayName("should return pods in time range")
            public void testSort() {
                var res = api.retrieveData(
                        api.POST(200, api.postServiceRequest(TimeRange.of(T1, T4), "aaa"))
                );
                res.assertPods(4);
            }

            @Test
            @DisplayName("should return only specified pods")
            public void testSearchPods() {
                var svc = new Requests.Service("aCdtControllerTest", "aCdtControllerTest");
                var res = api.retrieveData(
                        api.POST(200, api.postServiceRequest(TimeRange.of(T1, T4), "aaa", svc))
                );
                res.assertPods(2);
            }

            @Test
            @DisplayName("should return only specified pods in time range")
            public void testSearchPodsInSmallRange() {
                var svc = new Requests.Service("bCdtControllerTest", "bCdtControllerTest");
                var res = api.retrieveData(
                        api.POST(200, api.postServiceRequest(TimeRange.of(T3, T4), "aaa", svc))
                );
                res.assertPods(1);
            }

            @Test
            @DisplayName("should not return pods outside time range")
            public void testSearchPodsInRange() {
                var svc = new Requests.Service("aCdtControllerTest", "aCdtControllerTest");
                var res = api.retrieveData(
                        api.POST(200, api.postServiceRequest(TimeRange.of(T3, T4), "aaa", svc))
                );
                res.assertPods(0);
            }

            @Test
            @DisplayName("should validate time range in filter")
            public void testIncorrectTimeRange() {
                api.POST(400, api.postServiceRequest(TimeRange.of(T4, T3), "aaa"));
            }

            @Test
            @DisplayName("should validate data in incoming json")
            public void testActivePods() {
                api.POST(400,
                        api.jsonRequest(" [ {\"timeRange\": { \"from\": 1656583200000, \"to\": 1656586800000 } } ] "));
            }

        }

        @Nested
        @DisplayName("when searching for dumps")
        class DumpsTests {
            final Dumps api = new Dumps();

            @Test
            @DisplayName("should return dumps in time range")
            public void testDumpsTimeRange() {
                test.setTime(T2);
                var res = api.retrieveData(api.POST(200,
                        api.request("aCdtControllerTest", "aCdtControllerTest", TimeRange.of(T1, T4), "aaa", 10, 1)
                ));
//                res.assertPods(1);
                res.assertJson("""                        
                        [{
                            "namespace":"aCdtControllerTest",
                            "service":"aCdtControllerTest",
                            "pod":"aCdtControllerTest-CdtControllerTest-1",
                            "podId":"aCdtControllerTest-CdtControllerTest-1_1624846767000",
                            "tags":["java"], 
                            "startTime":1624846767000,"onlineNow":true,"lastAck":1624846767000,
                            "dataAvailableFrom":1624846767000,"dataAvailableTo":1624846947000,
                            "downloadOptions":[
                                {"typeName":"top","uri":"/cdt/v2/dumps/aCdtControllerTest-CdtControllerTest-1_1624846767000/top/download?timeFrom=1624846767000&timeTo=1624846947000"},
                                {"typeName":"td","uri":"/cdt/v2/dumps/aCdtControllerTest-CdtControllerTest-1_1624846767000/td/download?timeFrom=1624846767000&timeTo=1624846947000"}
                            ]
                        }]""");
            }

            @Test
            @DisplayName("should correct calculate online field")
            public void testDumpsOnline() {
                test.setTime(T4);
                var res = api.retrieveData(api.POST(200,
                        api.request("aCdtControllerTest", "aCdtControllerTest", TimeRange.of(T1, T4), "aaa", 10, 1)
                ));
                res.assertPods(1);
                res.assertJson("""
                        [{
                            "namespace":"aCdtControllerTest",
                            "service":"aCdtControllerTest",
                            "pod":"aCdtControllerTest-CdtControllerTest-1",
                            "podId":"aCdtControllerTest-CdtControllerTest-1_1624846767000",
                            "tags":["java"],
                            "startTime":1624846767000,"onlineNow":false,"lastAck":1624846767000,
                            "dataAvailableFrom":1624846767000,"dataAvailableTo":1624848447000,
                            "downloadOptions":[
                                {"typeName":"top","uri":"/cdt/v2/dumps/aCdtControllerTest-CdtControllerTest-1_1624846767000/top/download?timeFrom=1624846767000&timeTo=1624848447000"},
                                {"typeName":"td","uri":"/cdt/v2/dumps/aCdtControllerTest-CdtControllerTest-1_1624846767000/td/download?timeFrom=1624846767000&timeTo=1624848447000"}
                            ]
                        }]""");
            }

            @Test
            @DisplayName("should return dumps in time range")
            public void testDump2s() {
                var res = api.retrieveData(api.POST(200,
                        api.request("aCdtControllerTest", "aCdtControllerTest", TimeRange.of(T1, T4), "aaa", 10, 1)
                ));
            }

            @Test
            @DisplayName("should validate data in incoming json")
            public void testDumpsJson() {
                api.POST(400,
                        api.request("aCdtControllerTest", "aCdtControllerTest", """
                                {
                                  "timeRange": {
                                    "from": 1694424286370,
                                    "to": 1693990237980
                                  },
                                  "query": "",
                                  "limit": 50,
                                  "page": 1
                                }
                                """));
            }

            @Test
            @DisplayName("should validate time range in filter")
            public void testIncorrectTimeRange() {
                api.POST(400, api.request("a", "b", TimeRange.of(T4, T3), "aaa", 10, 1));
            }

            @Test
            @DisplayName("should validate paging parameters")
            public void testIncorrectPaging() {
                api.POST(400, api.request("a", "b", TimeRange.of(T1, T4), "aaa", -1, 1));
                api.POST(400, api.request("a", "b", TimeRange.of(T1, T4), "aaa", 0, 1));
                api.POST(400, api.request("a", "b", TimeRange.of(T1, T4), "aaa", 5, -1));
                api.POST(400, api.request("a", "b", TimeRange.of(T1, T4), "aaa", 5, 0));
            }

            @Test
            @DisplayName("should download dump by id")
            public void testDownloadOneFile() throws IOException {
                var res = api.download.retrieveData(api.download.GET(200,
                        api.download.fileRequest(pod1.getPodId(), StreamType.TOP, 0)
                ));
                res.assertSize(DATA.getSize(StreamType.TOP));
                res.assertSame(DATA.getBytes(StreamType.TOP));
            }

            @Test
            @DisplayName("should download dumps in range")
            public void testDownloadRange() throws IOException {
                var res = api.download.retrieveData(api.download.GET(200,
                        api.download.zipRequest(TimeRange.of(T1, T4), pod1.getPodId(), StreamType.TOP)
                ));
                res.assertSize(1492);
                // TODO check archive (with several files)
//                res.assertSame(DATA.getBytes(StreamType.TOP));
            }

            @Test
            @DisplayName("should return 404 if dump not found")
            public void testDownloadNotFound() {
                api.download.retrieveData(api.download.GET(404,
                        api.download.fileRequest(pod1.getPodId(), StreamType.TOP, 1)
                ));
            }

        }

    }

    @Nested
    @DisplayName("/v2/heaps-controller")
    class HeapController {

        @Nested
        @DisplayName("when searching for heap dumps")
        @Execution(SAME_THREAD)
        class HeapDumpsTests {
            final HeapDumps api = new HeapDumps();

            @Test
            @DisplayName("should return dumps in time range")
            public void testDumpsTimeRange() {
                test.setTime(T2);
                var res = api.retrieveData(api.POST(200,
                        api.request(TimeRange.of(T1, T4), "aaa", 10, 1)
                ));
                res.assertReports(2);
                res.assertJson("""
                        [{
                            "namespace":"bCdtControllerTest",
                            "service":"bCdtControllerTest",
                            "pod":"bCdtControllerTest-CdtControllerTest-4",
                            "startTime":1624848447000,
                            "dumpId":0,
                            "creationTime":1624848447000,
                            "bytes":1456
                        },{
                            "namespace":"aCdtControllerTest",
                            "service":"bCdtControllerTest",
                            "pod":"bCdtControllerTest-CdtControllerTest-3",
                            "startTime":1624848087000,
                            "dumpId":0,
                            "creationTime":1624848087000,
                            "bytes":1456
                        }]""");
            }

            @Test
            @DisplayName("should download dump by id")
            public void testDownload() throws IOException {
                var podId = pod3.getPodId();
                var res = api.download.retrieveData(api.download.GET(200,
                        api.download.fileRequest(podId, StreamType.HEAP, 0)
                ));
                res.assertSize(DATA.getSize(StreamType.HEAP));
                res.assertSame(DATA.getBytes(StreamType.HEAP));
            }

            @Test
            @DisplayName("should delete dump by id")
            public void testDelete() throws IOException {
                var podId = pod4.getPodId();

                var resReq = api.download.retrieveData(api.download.GET(200,
                        api.download.fileRequest(podId, StreamType.HEAP, 0)
                ));
                resReq.assertSize(DATA.getSize(StreamType.HEAP));
                resReq.assertSame(DATA.getBytes(StreamType.HEAP));

                var resDelete = api.download.retrieveData(api.download.DELETE(200,
                        api.download.deleteRequest(podId, StreamType.HEAP, 0)
                ));
                resDelete.assertSame("DONE".getBytes());

                test.flushIndexes();
                test.sleep(1000); // because of delayed index re-flushing for OpenSearch

                api.download.retrieveData(api.download.GET(404,
                        api.download.fileRequest(podId, StreamType.HEAP, 0)
                ));
            }


        }
    }


}
