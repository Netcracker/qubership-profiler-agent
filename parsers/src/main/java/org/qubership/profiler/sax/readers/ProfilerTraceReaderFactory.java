package org.qubership.profiler.sax.readers;

import org.qubership.profiler.sax.raw.RepositoryVisitor;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class ProfilerTraceReaderFactory {

    ApplicationContext context;

    public ProfilerTraceReaderFactory(ApplicationContext context) {
        this.context = context;
    }

    public ProfilerTraceReader newTraceReader(RepositoryVisitor rv, String rootReference){
        return context.getBean(ProfilerTraceReader.class, rv, rootReference);
    }
}
