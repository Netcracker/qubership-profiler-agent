package com.netcracker.profiler.threaddump.parser;

/**
 * Contains information on lock line in stacktrace
 */
public class LockThreadLineInfo implements ThreaddumpParser.ThreadLineInfo {
    public String lookupType(String s) {
        s = s.trim();
        if (s.equals(TYPE_LOCKED))
            return TYPE_LOCKED;
        if (s.equals(TYPE_WAIT))
            return TYPE_WAIT;
        if (s.equals(TYPE_ENTRY))
            return TYPE_ENTRY;
        if (s.equals(TYPE_PARKING))
            return TYPE_PARKING;
        if (s.equals(TYPE_ELIMINATED))
            return TYPE_ELIMINATED;
        return s + "(unknown)";
    }

    public StringBuffer toJS(StringBuffer sb) {
        sb.append("new LockInfo('");
        sb.append(ThreaddumpParser.toJString(type)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(id)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(className));
        return sb.append("')\n");
    }

    public boolean isLockLine(String className) {
        return this.className.equals(className);
    }

    public boolean isMethodLine(String className, String methodName) {
        return false;
    }

    public String toString() {
        return "void " + className + "." + type + "() () []";//className + ";" + id + " (" + type + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LockThreadLineInfo that = (LockThreadLineInfo) o;

        if (className != null ? !className.equals(that.className) : that.className != null) return false;
//            if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;// = id != null ? id.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        return result;
    }

    public final static String TYPE_LOCKED = "locked";
    public final static String TYPE_WAIT = "waiting on";
    public final static String TYPE_ENTRY = "waiting to lock";
    public final static String TYPE_PARKING = "parking to wait for";
    public final static String TYPE_ELIMINATED = "eliminated";
    public String id, type, className;
}
