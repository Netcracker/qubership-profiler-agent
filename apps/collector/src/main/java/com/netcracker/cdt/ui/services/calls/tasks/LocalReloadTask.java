package com.netcracker.cdt.ui.services.calls.tasks;

import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.cdt.ui.models.PodsIndex;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.cdt.ui.services.calls.models.CallPodResult;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.search.InternalCallFilter;
import com.netcracker.common.models.TimeRange;
import io.quarkus.logging.Log;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * The main orchestrator task to retrieve and filter calls after getting request for search from UI
 * <br>
 */
public class LocalReloadTask implements ReloadTask{
    public final PodsIndex pods;// keep cache of pods' meta data
    private final boolean export;
    private final String windowId;
    private final CallsMetaLoader loader;
    private final int threads;
    private final CallsListRequest request;

    private final InternalCallFilter callFilter;
    private final TimeRange period;

    private ReloadTaskState taskState; // main "context" (keep state for current search)
    private List<PodMetaData> podList;
    private LinkedBlockingDeque<CallSeqResult> seqResultQueue;

    public LocalReloadTask(CallsMetaLoader metaLoader, int concurrent, CallsListRequest request, boolean export) {
        this.threads = concurrent;
        this.loader = metaLoader;
        this.export = export;
        this.pods = PodsIndex.create();
        this.request = request;

        callFilter = new InternalCallFilter(request.durationRange(), request.query());
        period = request.timeRange();
        windowId = request.windowId();
    }

    @Override
    public ReloadTaskState prepare() {
        podList = loader.findPods(pods, request);
        Log.infof("[%s] Found %d pods for request", windowId, podList.size());
 
        taskState = new ReloadTaskState(windowId, podList.size());
        if (!export) {
            seqResultQueue = new LinkedBlockingDeque<>(2 * threads);
        }
        return this.taskState;
    }

    @Override
    public void run() {
        if (taskState.shouldStop()) return; // against latecomers (after timeout, for example)

        try (var executor = MergeExecutor.PriorityExecutor(threads)) {
            var worker = new MergeExecutor<PodCallsRetriever, CallPodResult>(executor);
            executor.submit(() -> { // subscribe to queue
                processQueue(worker);
            });

            // submit tasks
            for (var pod : podList) {
                var task = new PodCallsRetriever(loader,
                        taskState, pod, period, callFilter,
                        this::proceedSeqResult);
                worker.register(task);
            }

            var start = System.currentTimeMillis();
            taskState.waitForResults();
            var ms = System.currentTimeMillis() - start;
            if (taskState.remainingPods() > 0) {
                Log.warnf("Returns first %d results in %d ms. Still count down %d of %d pods",
                        taskState.fetchedCalls(), ms, taskState.remainingPods(), taskState.totalPods());
            } else {
                Log.infof("Returns %d results in %d ms, got all info from %d pods",
                        taskState.fetchedCalls(), ms, taskState.totalPods());
            }
        }
    }

    protected void proceedSeqResult(CallSeqResult podSeqResult) {
        if (seqResultQueue == null) return;

        var task = podSeqResult.subTask();
        if (task != null) {
            Log.debugf("[%s] Push to queue result %s with %d calls", task.podId(), task, podSeqResult.parsedCalls());
            seqResultQueue.offerLast(podSeqResult);
        }
    }

    private void processQueue(MergeExecutor<PodCallsRetriever, CallPodResult> worker) {
        if (seqResultQueue == null) return;

        try {
            while (true) {
                var el = seqResultQueue.pollFirst(100, TimeUnit.MILLISECONDS);
                if (el == null) {
                    if (taskState.shouldStop()) {
                        break;
                    }
                    if (taskState.nothingLeft()) {
                        break;
                    }
                } else {
                    Log.debugf("Got result %s with %d calls", el.subTask(), el.parsedCalls());
                    if (taskState.appendResult(el)) { // should stop
                        worker.cancelRunning();
                    }
                }
            }
        } catch (InterruptedException e) {
            Log.errorf(e, "problem?");
            throw new RuntimeException(e);
        } finally {
            taskState.finish();
            if (taskState.remainingPods() > 0) {
                Log.infof("[%s] Done? %d%% Count down: waiting %d of %d pods",
                        taskState.windowId(), taskState.percent(), taskState.remainingPods(), taskState.totalPods());
            } else {
                Log.infof("[%s] Done? %d%% from %d pods",
                        taskState.windowId(), taskState.percent(), taskState.totalPods());
            }
        }
    }
}
