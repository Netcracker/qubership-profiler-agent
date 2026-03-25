package com.netcracker.cdt.collector.tcp;

import java.io.IOException;

public interface AgentConnection {

    boolean commandAvailable() throws IOException;

    boolean isSocketDead();

    String getConnectionName();

    void close(String reason);
}
