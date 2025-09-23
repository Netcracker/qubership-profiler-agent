package com.netcracker.profiler.instrument;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeUtils {
    public static final Logger log = LoggerFactory.getLogger(TypeUtils.class);

    private final static Pattern MVN_URL = Pattern.compile("(?:wrap:)?mvn:([^/]++)/([^/]++)/([^/$]++)");

    public static String getFullJarName(ProtectionDomain pd){
        if (pd == null) return null;
        URI uri = null;
        try {
            final CodeSource source = pd.getCodeSource();
            if (source == null) return null;
            final URL location = source.getLocation();
            if (location == null) return null;
            String stringUri = null;
            try {
                uri = location.toURI();
                stringUri = uri.toString();
            } catch (java.net.URISyntaxException e) {
                stringUri = location.toString();
            }
            if (stringUri == null) return null;
            File file;
            if (stringUri.startsWith("jar:file:")) {
                int endIdx = stringUri.endsWith("!/") ? stringUri.length() - 2 : stringUri.length();
                file = new File(stringUri.substring("jar:file:".length(), endIdx));
            } else
            if (stringUri.startsWith("file:")) {
                file = new File(stringUri.substring("file:".length()));
            } else
            if (stringUri.startsWith("vfs:")) {
                file = new File(stringUri.substring("vfs:".length()));
            } else
            if (stringUri.startsWith("mvn:")) {
                return resolveMvnJar(stringUri);
            } else
            if (uri == null){
                return "unknown";
            } else {
                file = new File(uri);
            }
            String path = file.getAbsolutePath();
            if (File.separatorChar != '/')
                path = path.replace(File.separatorChar, '/');
            if (file.isDirectory())
                path += '/';
            return path;
        } catch (Exception t) {
            log.info("Unable to get jar name for protection domain " + pd + ", uri=" + uri, t);
        }
        return null;
    }

    public static String resolveMvnJar(String stringUri) {
        Matcher matcher = MVN_URL.matcher(stringUri);
        if (matcher.lookingAt()) {
            String group = matcher.group(1);
            String artifact = matcher.group(2);
            String version = matcher.group(3);
            return "system" + File.separatorChar
                    + group.replace('.', File.separatorChar) + File.separatorChar
                    + artifact + File.separatorChar
                    + version + File.separatorChar
                    + artifact + '-' + version + ".jar";
        }
        return stringUri;
    }

    public static String getJarName(ProtectionDomain pd) {
        String path = getFullJarName(pd);
        if (path == null) return null;
        try {
            int idx = path.lastIndexOf('/');
            for (int i = 0; i < 2 && idx > 0; i++) {
                int next = path.lastIndexOf('/', idx - 1);
                if (next == -1) break;
                idx = next;
            }
            return path.substring(Math.max(idx+1, 0));
        } catch (Exception t) {
            // ignore
        }
        return null;
    }

    public static String getMethodFullname(String methodName, String desc, String className, String sourceFileName, int firstLineNumber, String jarName) {
        StringBuilder sb = new StringBuilder(desc.length() + methodName.length() + className.length() + (jarName != null ? jarName.length() : 0) + 25);
        sb.append(Type.getReturnType(desc).getClassName()).append(' ');
        if (className != null) sb.append(className.replace('/', '.')).append('.');
        sb.append(methodName).append('(');
        for (Type type : Type.getArgumentTypes(desc))
            sb.append(type.getClassName()).append(',');

        final int lastIdx = sb.length() - 1;
        if (sb.charAt(lastIdx) != ',')
            sb.append(')');
        else
            sb.setCharAt(lastIdx, ')');
        sb.append(" (");
        sb.append(sourceFileName);
        sb.append(':');
        sb.append(firstLineNumber);
        sb.append(')');
        if (jarName != null) {
            sb.append(" [");
            sb.append(jarName);
            sb.append(']');
        }
        return sb.toString();
    }

    public static Object typeToFrameType(Type type) {
        switch (type.getSort()) {
            case Type.ARRAY:
                return type.toString();
            case Type.OBJECT:
                return type.getInternalName();
            case Type.DOUBLE:
                return Opcodes.DOUBLE;
            case Type.BOOLEAN:
            case Type.CHAR:
            case Type.INT:
            case Type.SHORT:
            case Type.BYTE:
                return Opcodes.INTEGER;
            case Type.FLOAT:
                return Opcodes.FLOAT;
            case Type.LONG:
                return Opcodes.LONG;
            default:
                return Opcodes.TOP; // should not happen
        }
    }

    public static void pushDefaultValue(GeneratorAdapter ga, Type type) {
        switch (type.getSort()) {
            case Type.BOOLEAN:
                ga.push(false);
                break;
            case Type.BYTE:
            case Type.CHAR:
            case Type.SHORT:
            case Type.INT:
                ga.push(0);
                break;
            case Type.LONG:
                ga.push(0L);
                break;
            case Type.FLOAT:
                ga.push(0f);
                break;
            case Type.DOUBLE:
                ga.push(0d);
                break;
            case Type.VOID:
                // nothing
                break;
            default:
                ga.push((Type) null);
        }

    }
}
