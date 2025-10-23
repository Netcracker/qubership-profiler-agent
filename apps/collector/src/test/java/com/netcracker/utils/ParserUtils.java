package com.netcracker.utils;

import com.netcracker.cdt.collector.parsers.DictionaryStreamParser;
import com.netcracker.cdt.collector.parsers.ParamsStreamParser;
import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.cdt.ui.services.calls.search.CallsExtractor;
import com.netcracker.cdt.ui.services.calls.search.PodSequenceParser;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.search.InternalCallFilter;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.streams.PodSequence;
import com.netcracker.profiler.model.Call;
import com.netcracker.profiler.sax.io.DataInputStreamEx;

import java.io.IOException;
import java.time.Instant;
import java.util.BitSet;
import java.util.List;

import static com.netcracker.utils.Utils.testRawDataStream;
import static com.netcracker.utils.Utils.testZipDataStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ParserUtils {

    public static CallSeqResult parseCallRecords(int expectedCalls, String callsFilename,
                                                 int expectedParams, String paramsFilename,
                                                 int expectedDicts, String dictFilename) throws IOException {
        List<DictionaryModel> dict;
        List<ParamsModel> params;
        try (var dis = testRawDataStream(dictFilename)) {
            dict = ParserUtils.parseDictionary(dis);
            assertNotNull(dict);
            assertEquals(expectedDicts, dict.size());
        }

        try (var dis = testRawDataStream(paramsFilename)) {
            params = ParserUtils.parseParams(dis);
            assertNotNull(params);
            assertEquals(expectedParams, params.size());
        }

        var pod = PodInfo.of("ns", "srv", "pod-name-aed4-orm_1675853926859");
        var podInfo = PodMetaData.empty(pod);
        podInfo.enrichDb(params, dict);

        try (var dis = testRawDataStream(callsFilename)) {
            var calls = parseCallRecords(dis, podInfo, TimeRange.ofEpochMilli(0, 1909257010000L));
            assertNotNull(calls);
            assertEquals(expectedCalls, calls.parsedCalls());
//            assertEquals(expectedCalls, calls.calls().toList().size());
            return calls;
        }
    }

    public static CallSeqResult parseCallRecords(DataInputStreamEx dis, PodMetaData podInfo, TimeRange period) throws IOException {
        var pid = PodIdRestart.of(podInfo.pod().oldPodName());
        var ps = new PodSequence(pid, 1, Instant.EPOCH, Instant.EPOCH);

        var task = new PodSequenceParser(period, podInfo, new InternalCallFilter(DurationRange.ofMillis(0, 1000000)));
        var result = task.parseSequenceStream(ps, dis);

        return result;
    }
    public static List<Call> parseCalls(String filename) throws IOException {
        try (var dis = testZipDataStream(filename)) {
            var filterer = new InternalCallFilter(DurationRange.ofSeconds(0, 1000000));
            return parseCalls(dis, filterer);
        }
    }

    public static List<Call> parseCalls(DataInputStreamEx dis, InternalCallFilter filterer) throws IOException {
        var ext = new CallsExtractor(dis, TimeRange.ofEpochMilli(0, 1689422758000L));
        var requiredTagIds = new BitSet();
        // parse to non-enriched entity (Call)
        var list = ext.findCallsInStream("testStream", requiredTagIds, filterer, -1);
        return list;
    }

    public static List<ParamsModel> parseParams(DataInputStreamEx dis) throws IOException {
        byte[] read = dis.readAllBytes();

        var pod = PodInfo.of("ns","service", "podName", Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);
        var parser = ParamsStreamParser.create(pod.restartId());
        parser.receiveData(read, 0, read.length, null);
        var params = parser.retrieveData();
        return params;
    }

    public static List<DictionaryModel> parseDictionary(DataInputStreamEx dis) throws IOException {
        byte[] read = dis.readAllBytes();

        var pod = PodInfo.of("ns","service", "podName", Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);
        var parser = DictionaryStreamParser.create(pod.restartId(), -1);
        parser.receiveData(read, 0, read.length, null);
        var params = parser.retrieveData();
        return params;
    }


}
