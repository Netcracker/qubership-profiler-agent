package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.io.exceptions.ErrorSupervisor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSupervisor extends ErrorSupervisor {
    public static final Logger log = LoggerFactory.getLogger(LoggingSupervisor.class);

    public static final LoggingSupervisor INSTANCE = new LoggingSupervisor();

    public void warn(String message, Throwable t) {
        log.warn(message, t);
    }

    public void error(String message, Throwable t) {
        log.error(message, t);
    }
}
