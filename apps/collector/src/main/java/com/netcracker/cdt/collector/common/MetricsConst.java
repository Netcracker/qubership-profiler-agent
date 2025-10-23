package com.netcracker.cdt.collector.common;

public interface MetricsConst {
    String PREFIX = "cdt.collector.";
    String CONNECTED_AGENTS = PREFIX + "agents.connected";
    String CONNECTED_AGENT_NAMESPACE = PREFIX + "agents.connected.namespace";
    String COMMAND_INIT_STREAM_EXECUTION_TIME = PREFIX + "command.init.stream.execution.time";
    String COMMAND_RCV_DATA_EXECUTION_TIME = PREFIX + "command.rcv.data.execution.time";
    String RECEIVED_BYTES = PREFIX + "received.bytes";
    String RECEIVED_FROM_AGENT_BYTES = PREFIX + "received.from.agent.bytes";
    String AGENT_UPTIME = PREFIX + "agent.uptime";
    String CLEANUP_COUNT = PREFIX + "cleanup.count";
    String CLEANUP_REPORT_CLEARED_BYTES = PREFIX + "cleanup.report.cleared.bytes";
    String CLEANUP_REPORT_EXECUTION_TIME = PREFIX + "cleanup.report.execution.time";
    String CLEANUP_REPORT_CLEARED_NAMESPACE_BYTES = PREFIX + "cleanup.report.cleared.namespace.bytes";
    String BUILD_INFO = PREFIX + "build.info";
    String NETWORK_TIME = PREFIX + "network.time";
    String STORAGE_TIME = PREFIX + "storage.time";
    String OWN_TIME = PREFIX + "own.time";

}
