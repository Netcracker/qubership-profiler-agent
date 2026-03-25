package com.netcracker.cdt.ui.services.calls.tasks;

import com.netcracker.cdt.ui.services.calls.CallsListResult;
import com.netcracker.cdt.ui.services.calls.view.CallsList;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import io.quarkus.logging.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.netcracker.cdt.ui.services.calls.models.CallSeqResult.Type.SUCCESS;

public class ReloadTaskState {
    // settings
    private int totalPods;
    private final String windowId;

    private CallsList calls;
    private int timeoutMs;
    private int uiFirstPageLimit;
    private int uiMaxLimit;

    // task state
    private final CountDownLatch done;
    private final CountDownLatch readyToSend;
    private boolean finished = false;

    // statistics
    private final Set<PodIdRestart> pods;

    private long fetchedCalls = 0;
    private long parsedCalls = 0;

    private int successfulSeq = 0;
    private int failedSeq = 0;
    private int timedOutSeq = 0;

    private final List<Throwable> exceptions = new ArrayList<>();

    public ReloadTaskState(String windowId, int podsSize) {
        this.windowId = windowId;
        this.totalPods = podsSize;

        this.done = new CountDownLatch(podsSize);
        this.readyToSend = new CountDownLatch(1);
        this.pods = new TreeSet<>();
    }

    public ReloadTaskState uiLimits(int uiMaxLimit, int uiFirstPageLimit) {
        this.uiMaxLimit = uiMaxLimit;
        this.uiFirstPageLimit = uiFirstPageLimit;
        return this;
    }

    public ReloadTaskState timeout(int maxRequestMs) {
        this.timeoutMs = maxRequestMs;
        return this;
    }

    public void setCallsList(CallsList callsList) {
        this.calls = callsList;
    }

    public CallsList getCallsList() {
        return calls;
    }

    public void recordFailure(Throwable exception){
        this.failedSeq++;
        this.exceptions.add(exception);
    }

    public void recordSuccess(long parsedCalls, long fetchedCalls, Set<PodIdRestart> pods){
        this.successfulSeq++;
        this.parsedCalls += parsedCalls;
        this.fetchedCalls += fetchedCalls;
        this.pods.addAll(pods);
        this.totalPods = podsCount();
    }

    public String windowId() {
        return this.windowId;
    }

    public void waitForResults() {
        if (timeoutMs == 0) {
            return;
        }
        try {
            readyToSend.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Log.errorf("error during wait", e);
        }
    }

    public void markPodAsDone() {
        done.countDown();
    }

    public boolean appendResult(CallSeqResult rs) {
        if (SUCCESS.equals(rs.res())) {
            this.calls.append(rs);
        }
        return this.shouldStop();
    }

    public boolean append(CallSeqResult res, int filteredSize) { // only successful results
        parsedCalls += res.parsedCalls(); // original calls retrieved and parsed from binary
        fetchedCalls += filteredSize; // filtered after first un-enriched data (InternalCallFilter)
        if (res.subTask() != null) {
            pods.add(res.subTask().pod());
        }

        switch (res.res()) {
            case SUCCESS -> successfulSeq++;
            case FAILED -> {
                failedSeq++;
                this.exceptions.addAll(res.exceptions());
            }
            case TIMEOUT -> timedOutSeq++;
        }
        if (fetchedCalls > uiFirstPageLimit) {
            if (readyToSend.getCount() > 0) {
                readyToSend.countDown();
            }
        }

        return shouldStop();
    }

    public synchronized void finish() {
        finished = true;
        if (readyToSend.getCount() > 0) {
            readyToSend.countDown();
        }
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean shouldStop() {
        return finished || fetchedCalls > uiMaxLimit;
    }

    public boolean nothingLeft() {
        return done.getCount() == 0;
    }

    public int percent() {
        return (int) (100 - remainingPods() * 100 / totalPods);
    }

    public long parsedCount() {
        return parsedCalls;
    }

    public int servicesCount() {
        var services = pods.stream().map(PodIdRestart::service).collect(Collectors.toSet());
        return services.size();
    }

    public long remainingPods() {
        return done.getCount();
    }

    public int totalPods() {
        return totalPods;
    }

    public int podsCount() {
        return pods.size();
    }

    public List<String> getExceptions() {
        return exceptions.stream().map(Throwable::getMessage).toList();
    }

    public synchronized long fetchedCalls() {
        return fetchedCalls;
    }

    public int getProgress() {
        return 0;
    }

    public CallsListResult.Status getStatus(long filteredCalls) {
        return new CallsListResult.Status(
                isFinished(), getProgress(),
                getExceptions(),
                filteredCalls, // filtered
                fetchedCalls, // processed and persisted in cache
                podsCount(), successfulSeq, timedOutSeq, failedSeq
        );
    }

}
