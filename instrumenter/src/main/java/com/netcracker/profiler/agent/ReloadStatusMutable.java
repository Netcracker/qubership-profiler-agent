package com.netcracker.profiler.agent;

public interface ReloadStatusMutable extends ReloadStatus {
    /**
     * Sets status of reload operation
     *
     * @param done status of reload operation
     */
    public void setDone(boolean done);

    /**
     * Total number of classes to be reloaded.
     * When reloading is complete it should be equal to the sum of successCount and errorCount
     *
     * @param totalCount total number of classes to be reloaded
     */
    public void setTotalCount(int totalCount);

    /**
     * Number of classes that failed to reload (e.g. unable to fetch source bytes)
     *
     * @param successCount number of classes that failed to reload (e.g. unable to fetch source bytes)
     */
    public void setSuccessCount(int successCount);

    /**
     * Number of classes that failed to reload (e.g. unable to fetch source bytes)
     *
     * @param errorCount number of classes that failed to reload (e.g. unable to fetch source bytes)
     */
    public void setErrorCount(int errorCount);

    /**
     * Set a message that describes current status (e.g. the name of jar name being processed)
     *
     * @param message message that describes current status (e.g. the name of jar name being processed)
     */
    public void setMessage(String message);

    /**
     * Sets the location of configuration file
     *
     * @param path location of configuration file
     */
    public void setConfigPath(String path);
}
