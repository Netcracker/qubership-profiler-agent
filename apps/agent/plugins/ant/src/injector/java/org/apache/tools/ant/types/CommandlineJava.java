package org.apache.tools.ant.types;

public class CommandlineJava {
    public native Commandline.Argument createVmArgument();
    public native String describeCommand();
    public native String getJar();
    public native String getClassname();
}
