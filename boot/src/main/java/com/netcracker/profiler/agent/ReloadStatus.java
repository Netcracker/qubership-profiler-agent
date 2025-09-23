package com.netcracker.profiler.agent;

public interface ReloadStatus {
    /**
     * Returns if the reloading is finished
     *
     * @return status of reload operation
     */
    public boolean isDone();

    /**
     * Total number of classes to be reloaded.
     * When reloading is complete it should be equal to the sum of successCount and errorCount
     *
     * @return total number of classes to be reloaded
     */
    public int getTotalCount();

    /**
     * Number of classes that failed to reload (e.g. unable to fetch source bytes)
     *
     * @return number of classes that failed to reload (e.g. unable to fetch source bytes)
     */
    public int getSuccessCount();

    /**
     * Number of classes that failed to reload (e.g. unable to fetch source bytes)
     *
     * @return number of classes that failed to reload (e.g. unable to fetch source bytes)
     */
    public int getErrorCount();

    /**
     * Get a message that describes current status (e.g. the name of jar name being processed)
     *
     * @return message that describes current status (e.g. the name of jar name being processed)
     */
    public String getMessage();

    /**
     * Returns the location of configuration file
     *
     * @return location of configuration file
     */
    public String getConfigPath();
}
