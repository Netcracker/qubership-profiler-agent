package com.netcracker.cdt.ui.services.calls.tasks;

import com.netcracker.cdt.ui.services.calls.models.CallPodResult;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.cdt.ui.services.calls.search.InternalCallFilter;
import com.netcracker.cdt.ui.services.calls.search.PodSequenceParser;
import com.netcracker.common.models.pod.streams.PodSequence;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.utils.DB;
import com.netcracker.profiler.timeout.ReadInterruptedException;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Task to parse all binary sequences related to the pod
 *
 * @param podInfo        original pod info (would be enriched with metadata during task)
 * @param period         time period from UI request
 * @param callFilter     filter for raw representation (without enriched tags)
 * @param dbLoader       db helper
 * @param podSeqListener listener for each completed PodSequence task
 */
public record PodCallsRetriever(CallsMetaLoader dbLoader,
                                ReloadTaskState taskState,
                                PodMetaData podInfo, TimeRange period,
                                InternalCallFilter callFilter,
                                Consumer<CallSeqResult> podSeqListener)
        implements MergeExecutor.TaskWithPriority<CallPodResult> {

    @Override
    public long priority() {
        return podInfo.lastActive().toEpochMilli();
    }

    //  Main method for task
    @Override
    public CallPodResult call() throws Exception {
        Log.infof("[%s] start call loader tasks in %s", podInfo, period);
        if (!podInfo.isValid()) {
            Log.warnf("invalid pod name %s", podInfo);
            return (CallPodResult.empty());
        }

        var startTime = System.currentTimeMillis();
        CallPodResult res = null;
        try {
            res = find(period, podInfo);
            if (Log.isDebugEnabled()) {
                Log.debugf("[%s] Loaded %d call sequences", podInfo, res.foundSequences());
            }
            Log.debugf("[%s] Got pod result: %s", podInfo, res.res());
        } catch (ReadInterruptedException e) {
            Log.warnf("[%s] Failed to read calls in time", podInfo);
            res = (CallPodResult.failed(podInfo, -1, e));
        } catch (Exception e) {
            Log.errorf(e, "[%s] Exception when loading calls list", podInfo);
            res = (CallPodResult.failed(podInfo, -1, e));
        } finally {
            // after task completion (all sequences for this pod are finished)
            Log.infof("[%s] Done search for pod in %d ms", podInfo, System.currentTimeMillis() - startTime);
            taskState.markPodAsDone();
        }
        return res;
    }

    //  Finds all the calls that match filter criteria
    @DB("retriever.find")
    CallPodResult find(TimeRange period, PodMetaData podInfo) {
        var startTime = System.currentTimeMillis();
        Log.debugf("[%s] Looking for metadata for %s", podInfo, period);
        var loaded = dbLoader.findPodMetaData(podInfo);

        Log.debugf("[%s] Got %d tags in dictionary, %d params", podInfo, loaded.tagsSize(), loaded.paramsSize());

        var enriched = callFilter.enrich(podInfo.registeredLiterals());
        var seqParser = new PodSequenceParser(period, podInfo, enriched);
        int foundSequences = -1;
        int parsedCalls = 0;

        try {
            Log.debugf("[%s] Fetching calls in %s", podInfo, period);
            var sequences = dbLoader.callSequencesList(podInfo, period);
            foundSequences = sequences.size();
            Log.infof("[%s] Found %d sequences of calls stream to be fetched", podInfo, foundSequences);

            for (PodSequence sequence : sequences) {
                parsedCalls += loadSequence(sequence, seqParser);
            }

            Log.infof("[%s] Parsed %d calls from %d sequences in %d ms",
                    podInfo, parsedCalls, foundSequences,
                    System.currentTimeMillis() - startTime);
            //master thread will always interrupt after its timeout
            return (CallPodResult.success(podInfo, foundSequences));
//        } catch (ReadInterruptedException e) {
//            throw e;
//            return CallReaderResult.failed(e);
        } catch (Exception e) {
            //reactor core wraps it in reactor.core.Exceptions.ReactiveException
            var res = CallPodResult.failed(podInfo, foundSequences, e);
            if ((e.getCause() != null) && e.getCause() instanceof InterruptedException) {
                throw new ReadInterruptedException();
            }
            return res;
        }
    }

    // Load and parse pod sequence
    @DB("retriever.sequence")
    private int loadSequence(PodSequence sequence, PodSequenceParser seqParser) throws IOException {
        // TODO span subflow!
        if (Log.isDebugEnabled()) {
            Log.debugf("Parsing sequence: %s", sequence.toString());
        }
        var res = 0;
        try (var stream = dbLoader.getPodSequenceStream(sequence)) {
            try {
                var subResult = seqParser.parseSequenceStream(sequence, stream);
                if (Log.isDebugEnabled()) {
                    Log.debugf("Got result: %s", subResult.toString());
                }
                res += subResult.parsedCalls();
                podSeqListener.accept(subResult);
            } catch (Exception e) {
                Log.errorf(e, "[%s] Exception when attempting to read calls", sequence);
            }
        }
        return res;
    }
}
