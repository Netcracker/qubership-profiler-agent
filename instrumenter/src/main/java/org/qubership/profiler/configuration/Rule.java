package org.qubership.profiler.configuration;

import org.qubership.profiler.instrument.ProfileClassAdapter;
import org.qubership.profiler.instrument.ProfileMethodAdapter;
import org.qubership.profiler.instrument.custom.ClassAcceptor;
import org.qubership.profiler.instrument.custom.ClassAcceptorsList;
import org.qubership.profiler.instrument.custom.MethodAcceptor;
import org.qubership.profiler.instrument.custom.MethodAcceptorsList;
import org.qubership.profiler.tools.ListOfRegExps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Rule implements MethodAcceptor {
    private ArrayList<Pattern> classNames;
    private ArrayList<Matcher> classNameMatchers;
    private Boolean classNameHashable;
    private ArrayList<String> classNamesRaw;
    private ArrayList<Pattern> methodNames;
    private ArrayList<Pattern> excludedMethods;
    private ArrayList<Pattern> superClasses;
    private String ifEnhancer;
    private MethodAcceptorsList editors = null;
    private ClassAcceptorsList classEditors = null;
    private int methodModifiers = 0xffffff;
    private int classModifiers = 0xffffff;
    private int minimumMethodSize = 0;
    private int minimumMethodLines = 0;
    private int minimumMethodBackJumps = 0;
    private boolean doNotProfile = false;
    private boolean isStartEndPoint = false;
    private boolean isReactorPoint = false;
    private ConfigStackElement stack;

    public void doNotProfile() {
        doNotProfile = true;
    }

    public boolean shouldNotProfile() {
        return doNotProfile;
    }

    public boolean doesNotChangeClass() {
        return doNotProfile && editors == null && classEditors == null;
    }

    public boolean allMethodsMatch() {
        return minimumMethodSize == 0 &&
                minimumMethodLines == 0 &&
                minimumMethodBackJumps == 0 &&
                methodModifiers == 0xffffff &&
                classModifiers == 0xffffff &&
                methodNames == null &&
                excludedMethods == null;
    }

    public boolean hasSuperclassCriteria() {
        return superClasses != null;
    }

    public void setMinimumMethodSize(int minimumMethodSize) {
        this.minimumMethodSize = minimumMethodSize;
    }

    public void setMinimumMethodLines(int minimumMethodLines) {
        this.minimumMethodLines = minimumMethodLines;
    }

    public void setMinimumMethodBackJumps(int minimumMethodBackJumps) {
        this.minimumMethodBackJumps = minimumMethodBackJumps;
    }

    public boolean matches(int access, String className, String superClass, String[] interfaces) {
        if (classModifiers != 0xffffff && (access & classModifiers) == 0)
            return false;
        if (superClasses == null) return true;
        for (Pattern superClassPattern : superClasses)
            if (superClassPattern.matcher(superClass).matches())
                return true;
        if (interfaces != null)
            for (String _interface : interfaces)
                for (Pattern superClazz : superClasses)
                    if (superClazz.matcher(_interface).matches())
                        return true;
        return false;
    }

    public boolean matches(int access, String fullName, int methodCodeSize, int numberOfLines, int numberOfBackJumps) {
        if (methodModifiers != 0xffffff && (access & methodModifiers) == 0)
            return false;
        if (methodCodeSize < minimumMethodSize)
            return false;
        if (numberOfLines < minimumMethodLines)
            return false;
        if (numberOfBackJumps < minimumMethodBackJumps)
            return false;
        if (excludedMethods != null)
            for (Pattern methodName : excludedMethods)
                if (methodName.matcher(fullName).matches())
                    return false;
        if (methodNames == null) return true;
        for (Pattern methodName : methodNames) {
            if (methodName.matcher(fullName).matches())
                return true;
        }
        return false;
    }

    public void declareLocals(ProfileMethodAdapter ma) {
        if (editors == null) return;
        editors.declareLocals(ma);
    }

    public void onMethodEnter(ProfileMethodAdapter ma) {
        if (editors == null) return;
        editors.onMethodEnter(ma);
    }

    public void turnStartEndPoint() {
        isStartEndPoint = true;
    }

    public void turnReactorPoint() {
        isReactorPoint = true;
    }

    public void onMethodExit(ProfileMethodAdapter ma) {
        if (editors == null) return;
        editors.onMethodExit(ma);
    }

    public void onMethodException(ProfileMethodAdapter ma) {
        if (editors == null) return;
        editors.onMethodException(ma);
    }

    public void onClassEnd(ProfileClassAdapter ca, String className) {
        if (classEditors == null) return;
        classEditors.onClass(ca, className);
    }

    public ArrayList<Pattern> append(ArrayList<Pattern> list, String expr) {
        if (list == null) list = new ArrayList<Pattern>();
        StringBuffer sb = new StringBuffer(expr.length() + 20);
        argumentPatternToRegexp(sb, expr, false);
        log.debug("Class rule: {}", sb);
        list.add(Pattern.compile(sb.toString()));
        return list;
    }

    public void addClass(String className) {
        if (classNameHashable != Boolean.FALSE && className.indexOf('.') > -1 && className.indexOf('*') == -1) {
            classNameHashable = Boolean.TRUE;
            if (classNamesRaw == null)
                classNamesRaw = new ArrayList<String>();
            classNamesRaw.add(className);
        } else {
            classNameHashable = Boolean.FALSE;
            classNamesRaw = null;
        }
        classNames = append(classNames, className);
        if (classNameMatchers == null) classNameMatchers = new ArrayList<Matcher>();
        classNameMatchers.add(classNames.get(classNames.size() - 1).matcher(""));
    }

    public void setIfEnhancer(String ifEnhancer) {
        this.ifEnhancer = ifEnhancer;
    }

    public String getIfEnhancer() {
        return ifEnhancer;
    }

    public static Pattern METHOD_PATTERN = Pattern.compile("^([^( ]+)\\s*(\\(\\s*([^),]+(?:,[^),]+)*)?\\s*\\)?)?");
    public static Pattern SPECIALS = Pattern.compile("\\*\\*|\\*|\\.|\\$");
    public static Pattern METHOD_NAME_SPECIALS = Pattern.compile("\\*");
    public final static Logger log = LoggerFactory.getLogger(Rule.class);

    public final static Map<String, String> DESCRIPTORS = new HashMap<String, String>();

    static {
        DESCRIPTORS.put("void", "V");
        DESCRIPTORS.put("byte", "B");
        DESCRIPTORS.put("char", "C");
        DESCRIPTORS.put("double", "D");
        DESCRIPTORS.put("float", "F");
        DESCRIPTORS.put("int", "I");
        DESCRIPTORS.put("long", "J");
        DESCRIPTORS.put("short", "S");
        DESCRIPTORS.put("boolean", "Z");
        DESCRIPTORS.put("...", "(?:\\[*(?:L[^;]++;|[VBCDFIJSZ]))*");
        DESCRIPTORS.put("any", "\\[*(?:L[^;]++;|[VBCDFIJSZ])");
    }

    public static StringBuffer argumentPatternToRegexp(StringBuffer sb, String p, boolean convertAsJavaDescriptor) {
        if (p.charAt(0) == '^') {
            sb.append(p, 1, p.length());
            return sb;
        }
        log.debug("Converting pattern {}", p);
        int isArray = p.indexOf('[');
        if (isArray >= 0) {
            for (int i = isArray; i >= 0; i = p.indexOf('[', isArray + 1))
                sb.append('\\').append('[');
            while (Character.isSpaceChar(p.charAt(isArray - 1)))
                isArray--;
            p = p.substring(0, isArray);
        }

        final String simpleType = DESCRIPTORS.get(p);
        if (simpleType != null) {
            sb.append(simpleType);
            return sb;
        }

        if ("*".equals(p)) {
            if (convertAsJavaDescriptor)
                sb.append("(?:L[^;]++;|[VBCDFIJSZ])");
            else
                sb.append("[^;/]*");
            return sb;
        }
        if (convertAsJavaDescriptor) sb.append('L');

        if (p.indexOf('.') == -1) sb.append("(?:[^;]+/)?");
        Matcher m = SPECIALS.matcher(p);
        while (m.find()) {
            String s = m.group();
            if ("**".equals(s)) s = "[^;]*";
            else if ("*".equals(s)) s = "[^;/]*";
            else if (".".equals(s)) s = "/";
            else if ("$".equals(s)) s = "\\\\\\$";
            m.appendReplacement(sb, s);
        }
        m.appendTail(sb);
        if (convertAsJavaDescriptor) sb.append(';');
        return sb;
    }

    public static StringBuffer methodNamePatternToRegexp(StringBuffer sb, String p) {
        if (p.charAt(0) == '^') {
            sb.append(p);
            return sb;
        }
        log.debug("Converting method name pattern {}", p);
        Matcher m = METHOD_NAME_SPECIALS.matcher(p);
        while (m.find()) {
            m.appendReplacement(sb, "[^()]*");
        }

        m.appendTail(sb);
        return sb;
    }

    protected ArrayList<Pattern> addMethod(ArrayList<Pattern> r, String methodName) {
        if (r == null) r = new ArrayList<Pattern>();

        if (methodName.charAt(0) != '^') {
            final Matcher m = METHOD_PATTERN.matcher(methodName);
            if (!m.matches()) {
                throw new IllegalArgumentException("Unable to parse method name " + methodName);
            }
            StringBuffer sb = new StringBuffer(methodName.length() + 20);
            sb.append("^");
            methodNamePatternToRegexp(sb, m.group(1));
            sb.append("\\(");
            if (m.group(2) == null) { // just method name, no braces
                sb.append(".*"); // assume any kind of arguments
            } else if (m.group(3) != null) { // there is at least one argument
                for (String p : m.group(3).split("\\s*,\\s*"))
                    argumentPatternToRegexp(sb, p, true);
            }
            sb.append("\\).*");
            methodName = sb.toString();
        }
        log.debug("Method rule: {}", methodName);
        r.add(Pattern.compile(methodName));
        return r;
    }

    public void addIncludedMethod(String methodPattern) {
        methodNames = addMethod(methodNames, methodPattern);
    }

    public void addExcludedMethod(String methodPattern) {
        excludedMethods = addMethod(excludedMethods, methodPattern);
    }

    public void addSuperclass(String superClassName) {
        superClasses = append(superClasses, superClassName);
    }

    public void methodModifier(String modifier) {
        if (methodModifiers == 0xffffff) methodModifiers = 0;
        if (modifier == null) return;

        if (modifier.contains("public")) methodModifiers |= Modifier.PUBLIC;
        if (modifier.contains("protected")) methodModifiers |= Modifier.PROTECTED;
        if (modifier.contains("private")) methodModifiers |= Modifier.PRIVATE;
        if (modifier.contains("default")) methodModifiers |= Modifier.TRANSIENT;
        if (modifier.contains("package protected")) methodModifiers |= Modifier.TRANSIENT;
        if (modifier.contains("final")) methodModifiers |= Modifier.FINAL;
        if (modifier.contains("static")) methodModifiers |= Modifier.STATIC;
    }

    public void classModifier(String modifier) {
        if (classModifiers == 0xffffff) classModifiers = 0;
        if (modifier == null) return;

        if (modifier.contains("public")) classModifiers |= Modifier.PUBLIC;
        if (modifier.contains("protected")) classModifiers |= Modifier.PROTECTED;
        if (modifier.contains("private")) classModifiers |= Modifier.PRIVATE;
        if (modifier.contains("default")) classModifiers |= Modifier.TRANSIENT;
        if (modifier.contains("package protected")) classModifiers |= Modifier.TRANSIENT;
        if (modifier.contains("final")) classModifiers |= Modifier.FINAL;
        if (modifier.contains("static")) classModifiers |= Modifier.STATIC;
    }

    public void addMethodEditor(MethodAcceptor ma) {
        if (editors == null) editors = new MethodAcceptorsList();
        editors.add(ma);
    }

    public void editClass(ClassAcceptor ca) {
        if (classEditors == null) classEditors = new ClassAcceptorsList();
        classEditors.add(ca);
    }

    public boolean classNameMatches(String className) {
        if (classNames == null) return true;
        for (int i = 0; i < classNameMatchers.size(); i++) {
            Matcher matcher = classNameMatchers.get(i);
            synchronized (matcher) {
                matcher.reset(className);
                if (matcher.matches())
                    return true;
            }
        }
        return false;
    }

    public Collection<String> getClassNames() {
        if (!classNameHashable)
            return Collections.emptyList();
        return classNamesRaw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Rule");
        if (classNames != null) {
            sb.append(", names:[");
            for (Pattern className : classNames) {
                sb.append(className).append(',');
            }
            sb.append("]");
        }
        if (superClasses != null) {
            sb.append(",super:[");
            for (Pattern superClass : superClasses) {
                sb.append(superClass).append(',');
            }
            sb.append("]");
        }
        if (minimumMethodSize != 0)
            sb.append(',').append("min_method_size: ").append(minimumMethodSize);
        if (minimumMethodLines != 0)
            sb.append(',').append("min_method_lines: ").append(minimumMethodLines);
        if (minimumMethodBackJumps != 0)
            sb.append(',').append("min_method_back_jumps: ").append(minimumMethodBackJumps);
        if (methodNames != null) {
            sb.append(", method:[");
            for (Pattern methodName : methodNames) {
                sb.append(methodName).append(',');
            }
            sb.append("]");
        }
        if (excludedMethods != null) {
            sb.append(", exclude_method:[");
            for (Pattern methodName : excludedMethods) {
                sb.append(methodName).append(',');
            }
            sb.append("]");
        }
        return sb.toString();
    }

    public void setStackTraceAtCreate(ConfigStackElement currentStack) {
        stack = currentStack;
    }

    public ConfigStackElement getStackTraceAtCreate() {
        return stack;
    }

    public boolean isStartEndpoint() {
        return isStartEndPoint;
    }

    public boolean isReactorPoint() {
        return isReactorPoint;
    }

    @Override
    public int hashCode() {
        // Pattern does not override hashCode :(
        int result = ListOfRegExps.hashCode(classNames);
        result = 31 * result + ListOfRegExps.hashCode(methodNames);
        result = 31 * result + ListOfRegExps.hashCode(excludedMethods);
        result = 31 * result + ListOfRegExps.hashCode(superClasses);
        result = 31 * result + (editors != null ? editors.hashCode() : 0);
        result = 31 * result + (ifEnhancer != null ? ifEnhancer.hashCode() : 0);
        result = 31 * result + methodModifiers;
        result = 31 * result + classModifiers;
        result = 31 * result + minimumMethodSize;
        result = 31 * result + minimumMethodLines;
        result = 31 * result + minimumMethodBackJumps;
        result = 31 * result + (doNotProfile ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rule rule = (Rule) o;
        if (doNotProfile != rule.doNotProfile) return false;
        if (methodModifiers != rule.methodModifiers) return false;
        if (classModifiers != rule.classModifiers) return false;
        if (minimumMethodSize != rule.minimumMethodSize) return false;
        if (minimumMethodLines != rule.minimumMethodLines) return false;
        if (minimumMethodBackJumps != rule.minimumMethodBackJumps) return false;
        // Pattern does not override equals :(
        if (!ListOfRegExps.equals(classNames, rule.classNames)) return false;
        if (!ListOfRegExps.equals(excludedMethods, rule.excludedMethods)) return false;
        if (editors != null ? !editors.equals(rule.editors) : rule.editors != null) return false;
        if (ifEnhancer != null ? !ifEnhancer.equals(rule.ifEnhancer) : rule.ifEnhancer != null) return false;
        if (!ListOfRegExps.equals(methodNames, rule.methodNames)) return false;
        if (!ListOfRegExps.equals(superClasses, rule.superClasses)) return false;

        return true;
    }
}
