package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.instrument.TypeUtils;

import java.security.ProtectionDomain;

public abstract class ClassInfo {
    private String className;
    private String jarName;
    private ProtectionDomain protectionDomain;
    private boolean jarNameSet;

    public abstract String getJarAttribute(String name);

    public abstract String getJarSubAttribute(String entryName, String attrName);

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getJarName() {
        if (jarNameSet) {
            return jarName;
        }
        jarNameSet = true;
        String fullJarName = TypeUtils.getFullJarName(getProtectionDomain());
        setJarName(fullJarName);
        return jarName;
    }

    public void setJarName(String jarName) {
        this.jarName = jarName;
    }

    public ProtectionDomain getProtectionDomain() {
        return protectionDomain;
    }

    public void setProtectionDomain(ProtectionDomain protectionDomain) {
        this.protectionDomain = protectionDomain;
    }
}
