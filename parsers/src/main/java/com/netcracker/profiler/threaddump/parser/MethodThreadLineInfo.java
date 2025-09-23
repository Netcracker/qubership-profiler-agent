package com.netcracker.profiler.threaddump.parser;

/**
 * Contains information on method call in stacktrace
 */
public class MethodThreadLineInfo implements ThreaddumpParser.ThreadLineInfo {
    public StringBuffer toJS(StringBuffer sb) {
        sb.append("M.g('");
        sb.append(ThreaddumpParser.toJString(className)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(methodName)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(locationClass)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(locationLineNo));
        return sb.append("')\n");
    }

    public boolean isLockLine(String className) {
        return false;
    }

    public boolean isMethodLine(String className, String methodName) {
        return this.className.equals(className) && this.methodName.equals(methodName);
    }

    public String toString() {
        return (returnValue != null ? returnValue + ' ' : "void ") + className + "." + methodName + (arguments != null ? '(' + arguments + ") (" : "() (") + locationClass + ":" + locationLineNo + ") []";//className + ":" + methodName + "(" + locationClass + ":" + locationLineNo + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MethodThreadLineInfo that = (MethodThreadLineInfo) o;

        if (className != null ? !className.equals(that.className) : that.className != null) return false;
        if (locationClass != null ? !locationClass.equals(that.locationClass) : that.locationClass != null)
            return false;
        if (locationLineNo != null ? !locationLineNo.equals(that.locationLineNo) : that.locationLineNo != null)
            return false;
        if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
        result = 31 * result + (locationClass != null ? locationClass.hashCode() : 0);
        result = 31 * result + (locationLineNo != null ? locationLineNo.hashCode() : 0);
        return result;
    }

    public String className, methodName, locationClass, locationLineNo, arguments, returnValue;

    public void setClassName(String className) {
        if (className.startsWith("sun.reflect.GeneratedMethodAccessor"))
            className = "sun.reflect.GeneratedMethodAccessor";
        else if (className.startsWith("$Proxy"))
            className = "Proxy";
        this.className = className;
    }
}
