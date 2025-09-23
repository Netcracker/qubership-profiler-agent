package com.netcracker.profiler.sax.factory;

import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.builders.MultiRangeSuspendLogBuilder;
import com.netcracker.profiler.sax.builders.SuspendLogBuilder;
import com.netcracker.profiler.sax.readers.SuspendLogReader;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class SuspendLogFactory {
    private ApplicationContext context;

    public SuspendLogFactory(ApplicationContext context) {
        this.context = context;
    }

    public SuspendLog readSuspendLog(String folderReference) throws IOException {
        SuspendLogBuilder sb = context.getBean(SuspendLogBuilder.class, folderReference);
        SuspendLogReader sr = context.getBean(SuspendLogReader.class, sb, folderReference);

        sr.read();

        return sb.get();
    }

    public SuspendLog readMultiRangeSuspendLog(String folderReference, long begin, long end) throws IOException {
        MultiRangeSuspendLogBuilder sb = context.getBean(MultiRangeSuspendLogBuilder.class, folderReference, begin, end, context);
        SuspendLogReader sr = context.getBean(SuspendLogReader.class, sb, folderReference);

        sr.read();

        return sb.get();
    }

}
