package org.apache.activemq.transport.tcp;

import org.qubership.profiler.agent.LocalState;
import org.qubership.profiler.agent.ProfilerData;

import java.io.InterruptedIOException;

public class TcpTransport {
    protected void readCommandHandleException$profiler(LocalState state, Throwable t) {
        if (t instanceof InterruptedIOException) {
            state.event("1", ProfilerData.PARAM_CALL_IDLE);
        }
    }
}
