package com.netcracker.profiler.io;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"cassandrastorage","elasticsearchstorage"})
public class ParamReaderFactory {

    ApplicationContext context;

    public ParamReaderFactory(ApplicationContext context) {
        this.context = context;
    }

    public ParamReader getInstance(String rootReference) {
        return context.getBean(ParamReader.class, rootReference);
    }
}
