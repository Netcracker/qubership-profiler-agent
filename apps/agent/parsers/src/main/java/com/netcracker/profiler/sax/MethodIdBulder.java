package com.netcracker.profiler.sax;

public class MethodIdBulder {
    private StringBuilder sb = new StringBuilder();

    public String build(
            String packageName
            , String className
            , String methodName
            , String arguments
            , String returnType
            , String sourceFileName
            , String lineNumber
            , String jarName
    ) {
        StringBuilder sb = this.sb;
        sb.setLength(0);
        sb.append(returnType).append(' ');
        if (packageName != null && packageName.length() > 0)
            sb.append(packageName).append('.');
        sb.append(className).append('.');
        sb.append(methodName).append('(');
        sb.append(arguments);
        sb.append(") (");
        sb.append(sourceFileName);
        sb.append(':');
        sb.append(lineNumber);
        sb.append(')');
        if (jarName != null) {
            sb.append(" [");
            sb.append(jarName);
            sb.append(']');
        }
        return sb.toString();
    }

    public String build(String str) {
        return str;
    }
}
