package com.netcracker.common;

import java.util.*;

public interface Consts {

    int CALL_HEADER_MAGIC = 0xfffefdfc;

    int C_TIME = 0;
    int C_DURATION = 1;
    int C_NON_BLOCKING = 2;
    int C_CPU_TIME = 3;
    int C_QUEUE_WAIT_TIME = 4;
    int C_SUSPENSION = 5;
    int C_CALLS = 6;
    int C_FOLDER_ID = 7;
    int C_ROWID = 8;
    int C_METHOD = 9;
    int C_TRANSACTIONS = 10;
    int C_MEMORY_ALLOCATED = 11;
    int C_LOG_GENERATED = 12;
    int C_LOG_WRITTEN = 13;
    int C_FILE_TOTAL = 14;
    int C_FILE_WRITTEN = 15;
    int C_NET_TOTAL = 16;
    int C_NET_WRITTEN = 17;
    int C_PARAMS = 18;

    Map<String, Integer> UI_COLUMNS = Map.of("ts", C_TIME,
            "duration", C_DURATION, "cpuTime", C_CPU_TIME, "suspend", C_SUSPENSION, "queue", C_QUEUE_WAIT_TIME,
            "calls",  C_CALLS, "transactions", C_TRANSACTIONS,
            "diskBytes", C_FILE_TOTAL, "netBytes", C_NET_TOTAL, "memoryUsed", C_MEMORY_ALLOCATED);

    int TAGS_ROOT = -1;
    int TAGS_HOTSPOTS = -2;
    int TAGS_PARAMETERS = -3;
    int TAGS_CALL_ACTIVE = -4;
    int TAGS_JAVA_THREAD = -5;

    String CALLS_IDLE = "calls.idle";
    String ASYNC_ABSORBED = "async.absorbed";
    String WEB_URL = "web.url";
    String JAVA_THREAD = "java.thread";

    List<String> KNOWN_IDLE_URLS = Arrays.asList(
            "/actuator/ready",
            "/actuator/health",
            "/actuator/metrics",
            "/actuator/prometheus",
            "/probes/live",
            "/probes/ready"
    );


    // hide as system method
    Set<String> KNOWN_IDLE_METHODS = new HashSet<>(Arrays.asList(
            "com.netcracker.ejb.cluster.messages.MessageThread.run",
            "com.netcracker.ejb.cluster.DatabaseThread.run",
            "com.netcracker.ejb.cluster.NodeManagerThread.run",
            "com.netcracker.ejb.cluster.NotificationThread.run",
            "com.netcracker.ejb.cluster.RecoveryThread.run",
            "com.netcracker.platform.scheduler.impl.ncjobstore.NCJobStore$RecoveryLockManager.run",
            "com.netcracker.mediation.dataflow.impl.util.trigger.socket.SocketListenerThread.run",
            "com.netcracker.mediation.dataflow.impl.util.recovery.RecoveryThread.run",
            "netscape.ldap.LDAPConnThread.run",
            "org.quartz.impl.jdbcjobstore.JobStoreSupport$MisfireHandler.run",
            "org.quartz.impl.jdbcjobstore.JobStoreSupport$ClusterManager.run",
            "org.quartz.core.QuartzSchedulerThread.run",
            "oracle.jms.AQjmsConsumer.receiveFromAQ",
            "org.apache.tools.ant.taskdefs.StreamPumper.run",
            "weblogic.jms.bridge.internal.MessagingBridge.run",
            "java.net.SocketInputStream.read"
    ));
}
