package com.netcracker.profiler.test.pigs;

import com.netcracker.profiler.agent.StringUtils;

public class TransactionPig extends Exception {
    public final native Throwable getRollbackReason();

    public native int getStatus();
    Throwable rollbackReason;

    public synchronized Throwable getCause$profiler() {
        Throwable superCause = super.getCause();
        if (superCause == null && rollbackReason != null) {
            try {
                superCause = rollbackReason;
                initCause(rollbackReason);
            } catch (IllegalStateException e) {
            /* ignore "Can't overwrite cause" exception */
            }
        }
        return superCause;
    }

    public String getStatusAsString$profiler() {
        Throwable localThrowable = getRollbackReason();
        String str = localThrowable == null ? "Unknown" : StringUtils.throwableToString(localThrowable).toString();

        switch (getStatus()) {
            case 0:
                return "Active";
            case 7:
                return "Preparing";
            case 2:
                return "Prepared";
            case 9:
                return "Rolling Back. [Reason=" + str + "]";
            case 1:
                return "Marked rollback. [Reason=" + str + "]";
            case 8:
                return "Committing";
            case 3:
                return "Committed";
            case 4:
                return "Rolled back. [Reason=" + str + "]";
            case 5:
                return "Unknown";
            case 6:
        }
        return "****** UNKNOWN STATE **** : " + getStatus();
    }
}
