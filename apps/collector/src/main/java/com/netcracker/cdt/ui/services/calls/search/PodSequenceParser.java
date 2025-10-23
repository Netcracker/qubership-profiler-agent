package com.netcracker.cdt.ui.services.calls.search;

import com.netcracker.cdt.ui.services.calls.models.CallConverter;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.common.models.pod.streams.PodSequence;
import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.common.models.TimeRange;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;
import com.netcracker.profiler.model.CallFilterer;
import com.netcracker.profiler.timeout.ReadInterruptedException;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.util.BitSet;

/**
 * Parser for one block (usually, `5m`) of data from `calls` stream of specified pod.
 *
 * <br>
 * Appendix for `CallsExtractor`: enrich parsed calls with metadata (podId, dictionary).
 * Returns `CallSeqResult` (status and stream of already converted call records)
 *
 * @param period - required time period from user
 * @param podInfo - preloaded meta-data for the pod
 * @param callFilter - internal filter
 */
public record PodSequenceParser(TimeRange period, PodMetaData podInfo, CallFilterer<Call> callFilter) {

    public CallSeqResult parseSequenceStream(PodSequence podSequence, DataInputStreamEx is) {
//        ProfilerTimeoutHandler.checkTimeout(); // TODO create timeouts
        if (is == null || is.isEmpty()) {
            return CallSeqResult.empty();
        }

        var name = podInfo.oldPodName() + "." + podSequence.sequenceId();
        var worker = new CallsExtractor(is, period);
        try {

            BitSet requiredIDs = new BitSet();
            Log.debugf("[%s] trying to find calls in sequence", name);

            String seqId = Integer.toString(podSequence.sequenceId());
            var foundCalls = worker.findCallsInStream(seqId, requiredIDs, callFilter, Long.MAX_VALUE);
            if (!foundCalls.isEmpty()) {
                if (worker.callDataReader == null) {
                    Log.warnf("[%s] could not calculate call loader", name);
                }
            }

            var convertedCallRecords = CallConverter.convert(foundCalls.stream(), podInfo);

            Log.debugf("[%s] parsed %d calls, pass %d for transformation (with %d tags, %d params and %d required ids)",
                    podSequence.toString(), worker.getParsedCount(), worker.getResultCount(),
                    podInfo.tagsSize(), podInfo.registeredParams().size(), requiredIDs.size());
            return CallSeqResult.success(podSequence, worker.getParsedCount(), convertedCallRecords);

        } catch (ReadInterruptedException e) {
//            parentTask.status.onTimeout(podSequence);
//            return CallReaderResult.failed(podSequence, e);
            throw e;
        } catch (IOException e) {
            return CallSeqResult.failed(podSequence, worker.getParsedCount(), e);
        } catch (Exception e) {
            Log.errorf(e, "[%s] Exception when attempting to read calls", name);
            return CallSeqResult.failed(podSequence, worker.getParsedCount(), e);
        }
    }
}
