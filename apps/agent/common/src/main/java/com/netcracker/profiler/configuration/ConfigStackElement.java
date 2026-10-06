package com.netcracker.profiler.configuration;

/**
 * Holds stack trace of the path where the rule was created
 */
public class ConfigStackElement {
    public final String file;
    public final String tag;
    public final int position;
    public final ConfigStackElement parent;

    public ConfigStackElement(String tag, int position, String file, ConfigStackElement parent) {
        this.tag = tag;
        this.position = position;
        this.file = file;
        this.parent = parent;
    }

    public StringBuffer toString(StringBuffer sb) {
        if (sb.length() != 0) sb.append(", ");
        sb.append('<').append(tag).append("> #").append(position);
        if (parent == null || parent.file == null && file != null || !parent.file.equals(file))
            sb.append(" in file ").append(file);
        return sb;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (ConfigStackElement that = this; that != null; that = that.parent) {
            that.toString(sb);
        }
        return sb.toString();
    }
}
