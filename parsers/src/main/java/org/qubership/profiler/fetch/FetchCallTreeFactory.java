package org.qubership.profiler.fetch;

import org.qubership.profiler.io.CallRowid;
import org.qubership.profiler.output.CallTreeMediator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class FetchCallTreeFactory {
    @Autowired
    ApplicationContext context;

    public FetchCallTree fetchCallTree(CallTreeMediator mediator, CallRowid[] callIds, int paramsTrimSize) {
        return context.getBean(FetchCallTree.class, mediator, callIds, paramsTrimSize);
    }

    public FetchCallTree fetchCallTree(CallTreeMediator mediator, CallRowid[] callIds, int paramsTrimSize, long begin, long end){
        return context.getBean(FetchCallTree.class, mediator, callIds, paramsTrimSize, begin, end);
    }
}
