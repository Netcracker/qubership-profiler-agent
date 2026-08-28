package com.netcracker.profiler.agent;

public class DefaultMethodImplInfo {
    public final String className;
    public final String methodName;
    public final String methodDescr;
    public final String ifEnhancer;
    public final int access;
    public final boolean skipSuper;

    public DefaultMethodImplInfo(String className, String methodName, String methodDescr, String ifEnhancer,
                                 int access, boolean skipSuper) {
        this.className = className;
        this.methodName = methodName;
        this.methodDescr = methodDescr;
        this.ifEnhancer = ifEnhancer;
        this.access = access;
        this.skipSuper = skipSuper;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultMethodImplInfo that = (DefaultMethodImplInfo) o;

        if (access != that.access) return false;
        if (className != null ? !className.equals(that.className) : that.className != null) return false;
        if (methodDescr != null ? !methodDescr.equals(that.methodDescr) : that.methodDescr != null) return false;
        if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null) return false;
        if (ifEnhancer != null ? !ifEnhancer.equals(that.ifEnhancer) : that.ifEnhancer != null) return false;
        if (skipSuper != that.skipSuper) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
        result = 31 * result + (methodDescr != null ? methodDescr.hashCode() : 0);
        result = 31 * result + (ifEnhancer != null ? ifEnhancer.hashCode() : 0);
        result = 31 * result + access;
        result = 31 * result + (skipSuper ? 1 : 0);
        return result;
    }
}
