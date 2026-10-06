package com.netcracker.profiler.instrument;

import static com.netcracker.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import com.netcracker.profiler.agent.DumperConstants;
import com.netcracker.profiler.agent.LocalState;
import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.agent.StringUtils;
import com.netcracker.profiler.agent.TimerCache;
import com.netcracker.profiler.configuration.Rule;

import org.objectweb.asm.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ProfileMethodAdapter extends AdviceAdapter {
    private static final Logger log = LoggerFactory.getLogger(ProfileMethodAdapter.class);
    private final static Object[] EMPTY_STACK = new Object[0];

    private Label startTry = new Label();
    private Label catchThrowable = new Label();
    private final String fullName;
    private final Rule rule;
    private Type[] argumentTypes;
    private Type returnType;
    private final boolean constructor;

    public static final Type C_PROFILER = Type.getType(Profiler.class);
    public static final Method M_ENTER_RETURNING = Method.getMethod(LocalState.class.getName() + " enterReturning(int)");
    public static final Method M_EVENT = Method.getMethod("void event(Object,String)");
    public static final Method M_EVENT_ID = Method.getMethod("void event(Object,int)");
    public static final Method M_PLUGIN_EXCEPTION = Method.getMethod("void pluginException(Throwable)");
    public static final Method M_EXIT = Method.getMethod("void exit()");

    public static final Type C_OBJECT = Type.getType(Object.class);
    public static final Method M_TO_STRING = Method.getMethod("String toString()");

    public static final Type C_STRINGUTILS = Type.getType(StringUtils.class);
    public static final Method M_CONVERT = Method.getMethod("Object convert(Object)");

    public static final Type C_LOCAL_STATE = Type.getType(LocalState.class);

    public static final Type C_THROWABLE = Type.getType(Throwable.class);

    public static final Type C_TIMER_CACHE = Type.getType(TimerCache.class);

    private final String className;
    private int classVersion;
    private int throwable = -1; // local variable that holds Throwable that is caught from profiled method
    private int localState; // local variable that holds LocalState
    private int startTime; // local variable that holds method start time
    private int resultVariableNumber = -1; // local variable that holds method result value
    private int thisArgIndex; // local variable that holds "this" copy
    private int[] savedLocals; // log-parameter-when saves the argument at the method start, so this array relates old local -> new one

    /**
     * Creates a new {@link org.objectweb.asm.commons.AdviceAdapter}.
     *
     * @param mv        the method visitor to which this adapter delegates calls.
     * @param access    the method's access flags (see {@link org.objectweb.asm.Opcodes}).
     * @param className the name of parent class (e.g. java/lang/String).
     * @param name      the method's name.
     * @param desc      the method's descriptor (see {@link org.objectweb.asm.Type Type}).
     * @param fullName
     * @param rule
     */
    public ProfileMethodAdapter(MethodVisitor mv, int access, String className, String name, String desc, String fullName, Rule rule, int classVersion) {
        super(OPCODES_VERSION, mv, access, name, desc);
        if (rule.shouldNotProfile()) {
            if (log.isTraceEnabled())
                log.trace("Transforming method {} with do-not-profile rule loaded in {}", fullName, rule.getStackTraceAtCreate());
            else
                log.debug("Transforming method {} with do-not-profile rule", fullName);
        } else {
            if (log.isTraceEnabled())
                log.trace("Profiling method {} with rule loaded in {}", fullName, rule.getStackTraceAtCreate());
            else
                log.debug("Profiling method {}", fullName);
        }
        this.className = className;
        this.fullName = fullName;
        this.rule = rule;
        this.constructor = "<init>".equals(name);
        this.classVersion = classVersion;
    }

    private void doDeclareLocals() {
        if (!constructor && !rule.shouldNotProfile()) {
            localState = newLocal(C_LOCAL_STATE);
            // Profiler.enter(fullName)
            logEnter(fullName);
        }
        rule.declareLocals(this);
        // try {
        if (!constructor) {
            visitLabel(startTry);
        }
    }

    @Override
    public void visitCode() {
        super.visitCode();
        // TODO: allocate variable only when needed
        if (constructor) {
            doDeclareLocals();
        }
    }

    @Override
    protected void onMethodEnter() {
        if (!constructor) {
            doDeclareLocals();
        }
        rule.onMethodEnter(this);
    }

    @Override
    public void visitMaxs(int maxStack, int maxLocals) {
        if (constructor) {
            super.visitMaxs(maxStack, maxLocals);
            return;
        }
        // catch(Throwable t) {
        visitTryCatchBlock(startTry, catchThrowable, catchThrowable, "java/lang/Throwable");
        visitLabel(catchThrowable);

        // Frame is required for java 1.7 verifier
        // numLocal=0 ==> we do not care of the existing variables, and ASM would add "newLocal-added" ones anyway
        visitFrame(Opcodes.F_NEW, 0, null, 1, new Object[]{"java/lang/Throwable"});

        throwable = newLocal(C_THROWABLE);
        storeLocal(throwable);

        //   Profiler.exit()
        rule.onMethodException(this);
        if (!rule.shouldNotProfile())
            logExit();
        //   throw t
        loadLocal(throwable);
        throwException();
        // }
        super.visitMaxs(maxStack, maxLocals);
    }

    public int saveArg(int args) {
        int[] savedLocals = this.savedLocals;
        Type[] argumentTypes = getArgumentTypes();
        if (savedLocals == null) {
            savedLocals = new int[argumentTypes.length];
            this.savedLocals = savedLocals;
        }
        int prev = savedLocals[args];
        if (prev == 0) {
            prev = newLocal(argumentTypes[args]);
            savedLocals[args] = prev;
            loadArg(args);
            storeLocal(prev);
        }
        return prev;
    }

    public Object[] getMethodArgsAsLocals() {
        int isNotStatic = (methodAccess & ACC_STATIC) == 0 ? 1 : 0;

        Type[] argTypes = getArgumentTypes();
        Object[] locals = new Object[isNotStatic + argTypes.length];
        Arrays.fill(locals, 0, locals.length, Opcodes.TOP);
        if ((methodAccess & ACC_STATIC) == 0)
            locals[0] = getClassName();
        int index = isNotStatic;
        for (int i = 0; i < argTypes.length; i++, index++) {
            Type type = argTypes[i];
            locals[index] = TypeUtils.typeToFrameType(type);
        }

        return locals;
    }

    protected void onMethodExit(int opcode) {
        if (opcode != ATHROW) {
            rule.onMethodExit(this);
            if (!constructor && !rule.shouldNotProfile()) {
                logExit();
            }
        } else if (constructor) {
            // TODO: handle exceptions thrown from <init>
            // newLocal() can't be used here. All the locals must be allocated at the visitCode call
//            throwable = newLocal(C_THROWABLE);
//            storeLocal(throwable);
//            rule.onMethodException(this);
//            loadLocal(throwable);
//             We are before ATHROW, so it will throw the exception for us
//             throwException();
        }
    }

    public Type[] getArgumentTypes() {
        if (argumentTypes != null) return argumentTypes;
        return argumentTypes = Type.getArgumentTypes(methodDesc);
    }

    public Type getReturnType() {
        if (returnType != null) return returnType;
        return returnType = Type.getReturnType(methodDesc);
    }

    public String getMethodFullName() {
        return fullName;
    }

    /**
     * Returns name of parent class name in JVM format
     *
     * @return name of parent class name in JVM format (e.g. java/lang/String)
     */
    public String getClassName() {
        return className;
    }

    public void logEnter(String methodName) {
        push(ProfilerData.resolveTag(methodName) | DumperConstants.DATA_ENTER_RECORD);
        invokeStatic(C_PROFILER, M_ENTER_RETURNING);
        storeLocal(localState);
    }

    public void logEvent(String eventName, Type type, boolean convert) {
        if (type.getSort() != Type.OBJECT || !"java/lang/String".equals(type.getInternalName()))
            invokeStatic(C_STRINGUTILS, M_CONVERT);
        push(eventName);
        invokeStatic(C_PROFILER, M_EVENT);
    }

    public void logExit() {
//        loadLocal(localState);
//        if (!constructor) {
//            invokeVirtual(C_LOCAL_STATE, M_EXIT);
//            return;
//        }
//        Label localStateIsNull = new Label();
//        ifNull(localStateIsNull);
        loadLocal(localState);
        invokeVirtual(C_LOCAL_STATE, M_EXIT);
//        visitLabel(localStateIsNull);
//        Object[] locals = getMethodArgsAsLocals();
//        // Frame is required for java 1.7 verifier
//        visitFrame(Opcodes.F_NEW, locals.length, locals, 0, EMPTY_STACK);
    }

    public void getIntTime() {
        getStatic(C_TIMER_CACHE, "timer", Type.INT_TYPE);
    }

    public int getThrowableVariableNumber() {
        return throwable;
    }

    public int getLocalStateVariableNumber() {
        return localState;
    }

    public int getStartTimeVariableNumber() {
        return startTime;
    }

    public void setStartTimeVariableNumber(int startTime) {
        this.startTime = startTime;
    }

    public void declareResultVariable() {
        if (resultVariableNumber != -1)
            return;

        Type returnType = getReturnType();
        resultVariableNumber = newLocal(returnType);
        // Ensure local variable is initialized
        if (returnType.getSort() == Type.VOID) {
            throw new IllegalArgumentException("Unable to get result since method " + fullName + " returns VOID");
        }
        TypeUtils.pushDefaultValue(this, returnType);
        storeLocal(resultVariableNumber, returnType);
    }

    public void declareThisVariable() {
        if (thisArgIndex != 0) {
            return;
        }
        thisArgIndex = newLocal(Type.getObjectType(getClassName()));
        loadThis();
        storeLocal(thisArgIndex);
    }

    public int getSavedThisVariableNumber() {
        if (thisArgIndex == 0) {
            throw new IllegalStateException("thisArgIndex is not initialized. #declareThisVariable should be called to initialize it");
        }
        return thisArgIndex;
    }

    public void loadSavedThis() {
        loadLocal(getSavedThisVariableNumber());
    }

    public void stashResult() {
        if (resultVariableNumber == -1)
            throw new IllegalStateException(" resultVariableNumber is not initialized. #declareResultVariable should be called to initialize it");
        Type returnType = getReturnType();

        if (returnType.getSize() == 1)
            dup();
        else
            dup2();

        storeLocal(resultVariableNumber, returnType);
    }

    public int getResultVariableNumber() {
        if (resultVariableNumber == -1)
            throw new IllegalStateException("resultVariableNumber is not initialized. #declareResultVariable should be called to initialize it");
        return resultVariableNumber;
    }

    public int getClassVersion() {
        return classVersion;
    }
}
