package com.netcracker.cdt.collector.parsers;

import com.netcracker.common.models.meta.SuspendHickup;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.profiler.sax.visitors.ISuspendLogVisitor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SuspendLogParserVisitor implements ISuspendLogVisitor {
    public List<SuspendHickup> suspendHickupList = new ArrayList<>();
    public PodIdRestart pod;

    public SuspendLogParserVisitor(PodIdRestart pod) {
        this.pod = pod;
    }

    public void visitHiccup(long date, int delay) {
        suspendHickupList.add(createHickup(date, delay));
    }

    @Override
    public void visitEnd() {
    }

    public List<SuspendHickup> getAndClearSuspendHickupList() {
        List<SuspendHickup> suspendHickupList = new ArrayList<>(this.suspendHickupList);
        clearSuspendHickupList();
        return suspendHickupList;
    }

    public void clearSuspendHickupList() {
        suspendHickupList.clear();
    }

    public SuspendHickup createHickup(long date, int delay) {
        return new SuspendHickup(pod, Instant.ofEpochMilli(date), delay);
    }
}
