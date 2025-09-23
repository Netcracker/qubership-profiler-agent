package com.netcracker.profiler.dump;

import com.netcracker.profiler.Dumper;
import com.netcracker.profiler.IDumper;
import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_07;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.cloud.transport.ProfilerProtocolException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DumperThread extends Thread {
    private final static Logger log = LoggerFactory.getLogger(DumperThread.class);
    public static long DUMPER_RESTART_INTERVAL = TimeUnit.SECONDS.toMillis(Integer.getInteger(Dumper.class.getName() + ".RESTART_INTERVAL", 10));
    private final IDumper dumper;
    private volatile boolean shutdownRequested;
    public static volatile int dumperIncarnationWhenStarted = 0;
    private boolean remoteExceptionReported = false;

    final Thread SHUTDOWN_HOOK = new Thread() {
        @Override
        public void run() {
            shutdown();
        }
    };

    public DumperThread(IDumper dumper, String name) {
        super(name);
        this.dumper = dumper;
        setDaemon(true);
        log.debug("Starting dumper thread {}, version {}", name, Bootstrap.getImplementationVersion(getClass()));
        dumperIncarnationWhenStarted = ProfilerData.dumperIncarnation;
        start();
    }

    public void shutdown() {
        shutdownRequested = true;
        final DumperPlugin dumper = Bootstrap.getPlugin(DumperPlugin.class);
        if (dumper != null && dumper instanceof DumperPlugin_07){
            ((DumperPlugin_07) dumper).gracefulShutdown();
            return;
        }
        log.warn("Dumper does not support graceful shutdown, will just close the files");
        try {
            this.dumper.close();
        } catch (IOException e) {
            fatal("Unable to close dumper streams at shutdown", e);
        }
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);

        for (; !shutdownRequested; ProfilerData.dumperIncarnation++) {
            try {
                if (ProfilerData.dumperIncarnation != dumperIncarnationWhenStarted)
                    try {
                        Thread.sleep(DUMPER_RESTART_INTERVAL);
                    } catch (InterruptedException e) {
                        log.info("Interrupted while in delay between restarts");
                        interrupt(); // propagate exception
                        //exit the interrupted thread. there's nothing to clean up here
                        return;
                    }
                try {
                    dumper.initialize();
                    ProfilerData.dumperDead = false;
                    //reset the flag after successful initialization
                    remoteExceptionReported = false;
                    ProfilerData.warnBufferQueueOverflow = true;
                } catch (ProfilerProtocolException e) {
                    ProfilerData.dumperDead = true;
                    if (shouldReportRemoteException()) {
                        notFatal("Unable to connect to remote collector", e);
                        remoteExceptionReported = true;
                    }
                    continue;
                    //if it's already been reported,
                } catch (Throwable t) {
                    ProfilerData.dumperDead = true;
                    fatal("Unable to initialize dumper", t);
                    continue;
                }

                try {
                    dumper.dumpLoop();
                } catch (InterruptedException e) {
                    ProfilerData.dumperDead = true;
                    notFatal("interrupted dump loop", e);
                } catch (Throwable t) {
                    ProfilerData.dumperDead = true;
                    fatal("Error while dumping records", t);
                }
            } finally {
                if(dumper != null) {
                    try {
                        dumper.close();
                    } catch (Throwable t){
                        fatal("Error when closing dumper", t);
                    }
                }
            }
        }

        try {
            Runtime.getRuntime().removeShutdownHook(SHUTDOWN_HOOK);
        } catch (IllegalStateException e) {
            /* ignore "Shutdown in progress" */
        }
    }

    private boolean shouldReportRemoteException(){
        try{
            return !remoteExceptionReported || log.isDebugEnabled();
        } catch (Throwable t) {
            log.error("", t);
            return true;
        }
    }

    public void fatal(String message, Throwable t) {
        try {
            log.error(message, t);
        } catch (Throwable x) {
            try {
                System.err.println("[Profiler.DumperThread] Error writing to profiler.log");
                System.err.println(message);
                t.printStackTrace();
                x.printStackTrace();
            } catch (Throwable impossible) {
                /* should not get here */
            }
        }
    }

    public void notFatal(String message, Throwable t) {
        try {
            if(t == null) {
                log.info(message);
            } else {
                if(log.isDebugEnabled()) {
                    log.debug(message, t);
                } else {
                    log.info(message + " " + t.getMessage());
                }
            }
        } catch (Throwable x) {
            try {
                System.err.println("[Profiler.DumperThread] Error writing to profiler.log");
                System.err.println(message);
                if(t != null) {
                    t.printStackTrace();
                }
                x.printStackTrace();
            } catch (Throwable impossible) {
                /* should not get here */
            }
        }
    }
}
