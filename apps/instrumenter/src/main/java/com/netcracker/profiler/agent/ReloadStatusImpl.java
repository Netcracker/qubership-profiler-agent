package com.netcracker.profiler.agent;

public class ReloadStatusImpl implements ReloadStatusMutable {
    boolean done = true;
    int totalCount = 1;
    int successCount = 0;
    int errorCount = 0;
    String message;
    String configPath;

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(int successCount) {
        this.successCount = successCount;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String path) {
        configPath = path;
    }
}
