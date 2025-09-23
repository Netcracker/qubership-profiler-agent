package com.netcracker.profiler.agent;

public class MediationCallInfo {
    public Object rootSessionId;
    public String sessionId;
    public String elementId;
    public String module;

    public boolean setSessionId(String sessionId) {
        boolean res = StringUtils.stringDiffers(sessionId, this.sessionId);
        this.sessionId = sessionId;
        return res;
    }

    public boolean setElementId(String elementId) {
        boolean res = StringUtils.stringDiffers(elementId, this.elementId);
        this.elementId = elementId;
        return res;
    }

    public boolean setModule(String module) {
        boolean res = StringUtils.stringDiffers(module, this.module);
        this.module = module;
        return res;
    }
}
