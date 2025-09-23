package com.netcracker.profiler.agent;

public interface DumperPlugin_03 extends DumperPlugin_02 {
    /**
     * Returns Object[]{File dumpRoot, List&lt;InflightCall&gt; calls}
     * @return array with two elements: Object[]{File dumpRoot, List&lt;InflightCall&gt; calls}
     */
    public Object[] getInflightCalls();
}
