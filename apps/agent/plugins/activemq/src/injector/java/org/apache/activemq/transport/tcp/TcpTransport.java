package org.apache.activemq.transport.tcp;

import com.netcracker.profiler.agent.LocalState;
import com.netcracker.profiler.agent.ProfilerData;

import java.io.InterruptedIOException;

public class TcpTransport {
    protected void readCommandHandleException$profiler(LocalState state, Throwable t) {
        if (t instanceof InterruptedIOException) {
            state.event("1", ProfilerData.PARAM_CALL_IDLE);
        }
    }
}
