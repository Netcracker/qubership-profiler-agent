package com.netcracker.profiler.io.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ErrorCollector extends ErrorSupervisor {
    public List<ErrorMessage> errors = new ArrayList<ErrorMessage>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorCollector.class);

    public void warn(String message, Throwable t) {
        LOGGER.warn(message, t);
        errors.add(new ErrorMessage(Level.WARN, message, t));
    }

    public void error(String message, Throwable t) {
        LOGGER.error(message, t);
        errors.add(new ErrorMessage(Level.ERROR, message, t));
    }

    public List<ErrorMessage> getErrors() {
        return errors;
    }
}
