package com.netcracker.profiler.sax.factory;

import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.builders.MultiRangeSuspendLogBuilder;
import com.netcracker.profiler.sax.builders.MultiRangeSuspendLogBuilderFactory;
import com.netcracker.profiler.sax.builders.SuspendLogBuilder;
import com.netcracker.profiler.sax.builders.SuspendLogBuilderFactory;
import com.netcracker.profiler.sax.readers.SuspendLogReader;
import com.netcracker.profiler.sax.readers.SuspendLogReaderFactory;

import java.io.IOException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SuspendLogFactory {
    private final SuspendLogBuilderFactory suspendLogBuilderFactory;
    private final MultiRangeSuspendLogBuilderFactory multiRangeSuspendLogBuilderFactory;
    private final SuspendLogReaderFactory suspendLogReaderFactory;

    @Inject
    public SuspendLogFactory(SuspendLogBuilderFactory suspendLogBuilderFactory,
                             MultiRangeSuspendLogBuilderFactory multiRangeSuspendLogBuilderFactory,
                             SuspendLogReaderFactory suspendLogReaderFactory) {
        this.suspendLogBuilderFactory = suspendLogBuilderFactory;
        this.multiRangeSuspendLogBuilderFactory = multiRangeSuspendLogBuilderFactory;
        this.suspendLogReaderFactory = suspendLogReaderFactory;
    }

    public SuspendLog readSuspendLog(String folderReference) throws IOException {
        SuspendLogBuilder sb = suspendLogBuilderFactory.create(folderReference);
        SuspendLogReader sr = suspendLogReaderFactory.create(sb, folderReference);

        sr.read();

        return sb.get();
    }

    public SuspendLog readMultiRangeSuspendLog(String folderReference, long begin, long end) throws IOException {
        MultiRangeSuspendLogBuilder sb = multiRangeSuspendLogBuilderFactory.create(folderReference, begin, end);
        SuspendLogReader sr = suspendLogReaderFactory.create(sb, folderReference, begin, end);

        sr.read();

        return sb.get();
    }

}
