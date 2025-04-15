package org.qubership.profiler.instrument.custom.util;

import org.qubership.profiler.agent.Configuration_01;
import org.qubership.profiler.agent.LocalState;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.instrument.ProfileMethodAdapter;
import org.qubership.profiler.instrument.custom.MethodInstrumenter;
import org.qubership.profiler.util.XMLHelper;

import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ExecuteMethod extends MethodInstrumenter implements Opcodes {
    public static final Logger log = LoggerFactory.getLogger(ExecuteMethod.class);
    private static final Handle PROFILER_METAFACTORY_HANDLE = new Handle(H_INVOKESTATIC,
            "org/qubership/profiler/agent/ProfilerMetafactory", "catchPluginException",
            "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;)Ljava/lang/invoke/CallSite;", false);
    static int JAVA_7_VERSION = 51;
    protected String methodName;
    protected String className;
    protected List<Integer> args = Collections.emptyList();
    protected List<Type> argTypes = Collections.emptyList();
    protected Type returnType;
    protected boolean isStatic;
    protected boolean isPrivate;
    protected boolean getsResult;
    boolean addTryCatchBlocks = ProfilerData.ADD_TRY_CATCH_BLOCKS;

    public static final int RESULT_ARG_UNDEFINED = Integer.MIN_VALUE;
    protected int storeResultArg = RESULT_ARG_UNDEFINED;

    public static final Pattern METHOD_NAME_PATTERN = Pattern.compile("([^,) ]*?)\\s*(p\\d+|duration|startTime|state|result|this|throwable)\\s*[,)]", Pattern.CASE_INSENSITIVE);

    private static final Type C_LOCAL_STATE = Type.getType(LocalState.class);
    private static final Type C_THROWABLE = Type.getType(Throwable.class);

    public static final int ARG_START_TIME = -1;
    public static final int ARG_DURATION = -2;
    public static final int ARG_LOCAL_STATE = -3;
    public static final int ARG_RESULT = -4;
    public static final int ARG_THIS = -5;
    public static final int ARG_THROWABLE = -6;

    public final static Map<String, String> DESCRIPTORS = new HashMap<>();

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
    }

    protected static class MethodCallInfo {
        public final Method method;
        public final Type[] castFrom;

        public MethodCallInfo(Method method, Type[] castFrom) {
            this.method = method;
            this.castFrom = castFrom;
        }
    }

    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        String methodName = XMLHelper.getTextContent(e);
        if (methodName.length() == 0) {
            log.warn("Please, specify method name to call");
            return this;
        }

        String returnTypeStr = e.getAttribute("return");
        returnType = getType(returnTypeStr);

        methodName = parseMethodArgs(methodName);

        isStatic = Boolean.valueOf(e.getAttribute("static"));
        isPrivate = Boolean.valueOf(e.getAttribute("private"));
        this.className = extractTargetClassName(e);
        if (className == null)
            log.debug("Class name was not specified (via class or type argument) while parsing for method {}, assuming it is the same as the modified one", methodName);
        this.methodName = methodName;

        String storeResultArg = e.getAttribute("store-result-to-argument");
        if (storeResultArg.length() > 0) {
            try {
                this.storeResultArg = Integer.parseInt(storeResultArg);
                addTryCatchBlocks = false;
            } catch (NumberFormatException nfe) {
                /* ignore */
            }
        }
        if (this.storeResultArg != RESULT_ARG_UNDEFINED && Type.VOID_TYPE.equals(returnType)) {
            log.warn("store-result-to-argument is {}, however method {} return type is VOID", storeResultArg, returnType);
            this.storeResultArg = RESULT_ARG_UNDEFINED;
        }
        return this;
    }

    protected String parseMethodArgs(String methodName) {
        int argsStartPos = methodName.indexOf('(');
        if (argsStartPos != -1) {
            Matcher m = METHOD_NAME_PATTERN.matcher(methodName.substring(argsStartPos + 1));
            methodName = methodName.substring(0, argsStartPos);

            args = new ArrayList<Integer>();
            argTypes = new ArrayList<Type>();
            while (m.find()) {
                String argType = m.group(1);
                String paramName = m.group(2);
                Type dstArgType = getType(argType);

                parseArgument(paramName, dstArgType);
            }
            if (args.isEmpty()) args = Collections.emptyList();
        }
        return methodName;
    }

    private Type getType(String argType) {
        if (argType == null || argType.length() == 0)
            return null;

        StringBuilder typeDescriptor = new StringBuilder();
        if (argType.charAt(0) == '[' || (argType.indexOf('.') < 0 && argType.indexOf('[') < 0)) {
            String primitive = DESCRIPTORS.get(argType);
            if (primitive != null)
                argType = primitive;
            typeDescriptor.append(argType);
        } else {
            int isArray = argType.indexOf('[');
            if (isArray >= 0) {
                for (int i = isArray; i >= 0; i = argType.indexOf('[', isArray + 1))
                    typeDescriptor.append('[');
                while (Character.isSpaceChar(argType.charAt(isArray - 1)))
                    isArray--;
                argType = argType.substring(0, isArray);
            }
            String primitive = DESCRIPTORS.get(argType);
            if (primitive != null)
                typeDescriptor.append(primitive);
            else
                typeDescriptor.append('L').append(argType.replace('.', '/')).append(';');
        }
        return Type.getType(typeDescriptor.toString());
    }

    private void parseArgument(String paramName, Type dstArgType) {
        if ("duration".equalsIgnoreCase(paramName)) {
            args.add(ARG_DURATION);
            argTypes.add(Type.INT_TYPE);
            return;
        }
        if ("startTime".equalsIgnoreCase(paramName)) {
            args.add(ARG_START_TIME);
            argTypes.add(Type.INT_TYPE);
            return;
        }
        if ("state".equalsIgnoreCase(paramName)) {
            args.add(ARG_LOCAL_STATE);
            argTypes.add(C_LOCAL_STATE);
            return;
        }
        if ("throwable".equalsIgnoreCase(paramName)) {
            args.add(ARG_THROWABLE);
            argTypes.add(C_THROWABLE);
            return;
        }
        argTypes.add(dstArgType);
        if ("result".equalsIgnoreCase(paramName)) {
            args.add(ARG_RESULT);
            getsResult = true;
            return;
        }
        if ("this".equalsIgnoreCase(paramName)) {
            args.add(ARG_THIS);
            return;
        }
        args.add(Integer.parseInt(paramName.substring(1)) - 1);
    }

    private String extractTargetClassName(Element e) {
        String className = e.getAttribute("type");
        if (className.length() == 0)
            className = e.getAttribute("class");
        if (className.length() == 0)
            return null;
        return className.replace('.', '/');
    }

    protected void appendCall(ProfileMethodAdapter ma) {
        appendCall(ma, null, null);
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        if (getsResult)
            ma.declareResultVariable();
        if (!isStatic) {
            ma.declareThisVariable();
        }
        for (Integer arg : args) {
            if (arg >= 0) {
                ma.saveArg(arg);
            } else if (arg == ARG_THIS) {
                ma.declareThisVariable();
            }
        }
    }

    protected Type getReturnType(ProfileMethodAdapter ma) {
        if (returnType != null) {
            return returnType;
        }
        if (storeResultArg == RESULT_ARG_UNDEFINED) {
            return Type.VOID_TYPE;
        }
        return ma.getArgumentTypes()[storeResultArg];
    }

    protected void appendCall(ProfileMethodAdapter ma, Type[] additionalParams, int[] vars) {
        if (methodName == null) return;
        final Type targetClass = Type.getObjectType(className != null ? className : ma.getClassName());

        MethodCallInfo targetMethod = getTargetMethod(ma, additionalParams);

        if (!isStatic)
            ma.loadSavedThis();
        for (int i = 0, argsSize = args.size(); i < argsSize; i++) {
            Integer arg = args.get(i);
            generateLoadVar(ma, arg);
            if (targetMethod.castFrom == null) continue;
            generateCast(ma, targetMethod.castFrom[i], argTypes.get(i));
        }

        if (vars != null)
            for (int var : vars)
                ma.loadLocal(var);
        if (isStatic)
            if(shouldAddIndyTryCatchBlocks(ma.getClassVersion())) {
                invokeDynamicWithTryCatch(ma, H_INVOKESTATIC, targetClass.getInternalName(), targetMethod.method.getDescriptor());
            } else {
                ma.invokeStatic(targetClass, targetMethod.method);
            }
        else if (isPrivate) {
            if(shouldAddIndyTryCatchBlocks(ma.getClassVersion())) {
                invokeDynamicWithTryCatch(ma, H_INVOKESPECIAL, targetClass.getInternalName(), targetMethod.method.getDescriptor());
            } else {
                ma.invokeConstructor(targetClass, targetMethod.method); // We should append invokespecial, but there is no that method
            }
        } else {
            if(shouldAddIndyTryCatchBlocks(ma.getClassVersion())) {
                invokeDynamicWithTryCatch(ma, H_INVOKEVIRTUAL, targetClass.getInternalName(), targetMethod.method.getDescriptor());
            } else {
                ma.invokeVirtual(targetClass, targetMethod.method);
            }
        }
        Type returnType = targetMethod.method.getReturnType();
        storeResult(ma, returnType);
    }

    private void invokeDynamicWithTryCatch(ProfileMethodAdapter ma, int tag, String className, String methodDesc) {
        String hMethodDesc = methodDesc;
        if(tag != H_INVOKESTATIC) {
            methodDesc = "(L" + className + ";" + methodDesc.substring(1);
        }
        ma.invokeDynamic(methodName, methodDesc, PROFILER_METAFACTORY_HANDLE,
                new Handle(tag, className, methodName, hMethodDesc, tag == 9));
    }

    boolean shouldAddPlainTryCatchBlocks(int classVersion) {
        return addTryCatchBlocks && ProfilerData.ADD_PLAIN_TRY_CATCH_BLOCKS && !shouldAddIndyTryCatchBlocks(classVersion);
    }

    boolean shouldAddIndyTryCatchBlocks(int classVersion) {
        return addTryCatchBlocks && ProfilerData.ADD_INDY_TRY_CATCH_BLOCKS && classVersion >= JAVA_7_VERSION;
    }

    protected void storeResult(ProfileMethodAdapter ma, Type returnType) {
        if (Type.VOID_TYPE.equals(returnType)) {
            return;
        }
        if (storeResultArg == RESULT_ARG_UNDEFINED) {
            ma.visitInsn(Opcodes.POP + returnType.getSize() - 1);
        } else {
            ma.storeArg(storeResultArg);
        }
    }

    protected TryCatchData appendTry(ProfileMethodAdapter ma) {
        TryCatchData tryCatchData = new TryCatchData();
        ma.visitTryCatchBlock(tryCatchData.getStartTry(), tryCatchData.getEndTry(), tryCatchData.getStartCatch(), "java/lang/Throwable");
        ma.visitLabel(tryCatchData.getStartTry());
        return tryCatchData;
    }

    protected void appendCatch(ProfileMethodAdapter ma, Object[] locals, TryCatchData tryCatchData) {
        int localsLength = locals == null ? 0 : locals.length;
        ma.visitLabel(tryCatchData.getEndTry());
        ma.visitJumpInsn(ma.GOTO, tryCatchData.getEndCatch());
        ma.visitLabel(tryCatchData.getStartCatch());
        ma.visitFrame(Opcodes.F_NEW, localsLength, locals, 1, new Object[]{"java/lang/Throwable"});
        ma.invokeStatic(ProfileMethodAdapter.C_PROFILER, ProfileMethodAdapter.M_PLUGIN_EXCEPTION);
        ma.visitLabel(tryCatchData.getEndCatch());
        ma.visitFrame(Opcodes.F_NEW, localsLength, locals, 0, null);
    }

    private MethodCallInfo getTargetMethod(ProfileMethodAdapter ma, Type[] additionalParams) {
        Type[] requiredArgTypes;
        Type[] castFrom;
        Method targetMethod;
        requiredArgTypes = new Type[args.size() + (additionalParams != null ? additionalParams.length : 0)];
        final Type[] argTypes = ma.getArgumentTypes();
        castFrom = new Type[args.size()];
        for (int i = 0, argsSize = args.size(); i < argsSize; i++) {
            Integer arg = args.get(i);
            Type argType = null;
            if (arg >= 0)
                argType = argTypes[arg];
            else {
                switch (arg) {
                    case ARG_LOCAL_STATE:
                        argType = C_LOCAL_STATE;
                        break;
                    case ARG_RESULT:
                        argType = ma.getReturnType();
                        break;
                    case ARG_THIS:
                        argType = Type.getObjectType(ma.getClassName());
                        break;
                    case ARG_THROWABLE:
                        argType = C_THROWABLE;
                        break;
                }
            }
            Type overrideType = this.argTypes.get(i);
            if (overrideType != null) {
                castFrom[i] = argType;
                argType = overrideType;
            }
            requiredArgTypes[i] = argType;
        }
        if (additionalParams != null)
            System.arraycopy(additionalParams, 0, requiredArgTypes, args.size(), additionalParams.length);

        targetMethod = new Method(methodName, getReturnType(ma), requiredArgTypes);
        return new MethodCallInfo(targetMethod, castFrom);
    }

    private void generateLoadVar(ProfileMethodAdapter ma, int arg) {
        if (arg >= 0) {
            ma.loadLocal(ma.saveArg(arg));
            return;
        }
        switch (arg) {
            case ARG_START_TIME:
                ma.loadLocal(ma.getStartTimeVariableNumber());
                break;
            case ARG_DURATION:
                ma.getIntTime();
                ma.loadLocal(ma.getStartTimeVariableNumber());
                ma.math(GeneratorAdapter.SUB, Type.INT_TYPE);
                break;
            case ARG_LOCAL_STATE:
                ma.loadLocal(ma.getLocalStateVariableNumber());
                break;
            case ARG_RESULT:
                ma.loadLocal(ma.getResultVariableNumber());
                break;
            case ARG_THIS:
                ma.loadSavedThis();
                break;
            case ARG_THROWABLE:
                if (ma.getThrowableVariableNumber() < 0)
                    ma.visitInsn(Opcodes.ACONST_NULL);
                else
                    ma.loadLocal(ma.getThrowableVariableNumber());
                break;
        }
    }

    private void generateCast(ProfileMethodAdapter ma, Type fromType, Type toType) {
        if (fromType != null && toType != null) {
            if (fromType.getSort() != Type.ARRAY && fromType.getSort() != Type.OBJECT && fromType.getSort() != Type.METHOD) {
                // Primitive type
                ma.cast(fromType, toType);
            } else {
                // Object reference
                ma.checkCast(toType);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecuteMethod)) return false;
        if (!super.equals(o)) return false;

        ExecuteMethod that = (ExecuteMethod) o;
        if (isStatic != that.isStatic) return false;
        if (isPrivate != that.isPrivate) return false;
        if (storeResultArg != that.storeResultArg) return false;
        if (args != null ? !args.equals(that.args) : that.args != null) return false;
        if (argTypes != null ? !argTypes.equals(that.argTypes) : that.argTypes != null) return false;
        if (className != null ? !className.equals(that.className) : that.className != null) return false;
        if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null) return false;
        if (returnType != null ? !returnType.equals(that.returnType) : that.returnType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
        result = 31 * result + (returnType != null ? returnType.hashCode() : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (args != null ? args.hashCode() : 0);
        result = 31 * result + (argTypes != null ? argTypes.hashCode() : 0);
        result = 31 * result + storeResultArg;
        return result;
    }
}
