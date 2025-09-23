package com.netcracker.profiler.audit;

import org.slf4j.Logger;
import org.slf4j.MDC;

import javax.servlet.http.HttpSession;

public class AuditLog {
    protected final Logger log;

    public AuditLog(Logger log) {
        this.log = log;
    }

    public void info(String message, HttpSession session) {
        boolean needClear = MDC.get("req.remoteUser") == null;

        if (needClear)
            MDC.put("req.remoteUser", (String) session.getAttribute(UsernameFilter.PROFILER_REMOTE_USERNAME));
        try {
            log.info(message);
        } finally {
            if (needClear)
                clearMDC();
        }
    }

    public void trace(String message, HttpSession session) {
        boolean needClear = MDC.get("req.remoteUser") == null;

        if (needClear)
            MDC.put("req.remoteUser", (String) session.getAttribute(UsernameFilter.PROFILER_REMOTE_USERNAME));
        try {
            log.trace(message);
        } finally {
            if (needClear)
                clearMDC();
        }
    }

    public void info(String message) {
        log.info(message);
    }

    public void trace(String message) {
        log.info(message);
    }

    protected void clearMDC() {
        MDC.remove("req.remoteUser");
    }
}
