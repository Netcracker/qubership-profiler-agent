package com.netcracker.cdt.collector;

import com.netcracker.cdt.collector.tcp.CollectorConnectionAcceptor;
import com.netcracker.cdt.collector.tcp.CollectorOrchestratorThread;
import com.netcracker.common.ProtocolConst;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.IOException;

@Startup
@ApplicationScoped
@LookupIfProperty(name = "service.type", stringValue = "collector")
public class CollectorServer {

    @Inject
    CollectorOrchestratorThread orchestrator;
    @Inject
    CollectorConnectionAcceptor acceptor;

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        Log.infof("Starting CollectorServer threads");
        acceptor.start();
        orchestrator.start();
    }
}
