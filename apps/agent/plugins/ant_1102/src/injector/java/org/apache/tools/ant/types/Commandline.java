package org.apache.tools.ant.types;

public class Commandline {
    public native String describeCommand();
    public static class Argument {
        public native void setValue(String value);
    }
}
