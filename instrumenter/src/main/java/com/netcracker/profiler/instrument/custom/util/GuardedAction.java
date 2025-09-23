package com.netcracker.profiler.instrument.custom.util;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.TypeUtils;
import com.netcracker.profiler.instrument.custom.MethodAcceptor;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class GuardedAction extends MethodInstrumenter {
    public static final Logger log = LoggerFactory.getLogger(GuardedAction.class);

    private int minTime = Integer.MAX_VALUE;

    boolean exceptionOnly;
    boolean exception;

    protected final MethodAcceptor delegate;

    public GuardedAction(MethodAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);

        String minDuration = e.getAttribute("duration-exceeds");
        if (minDuration.length() == 0)
            minDuration = e.getAttribute("duration-exeeds");
        if (minDuration.length() > 0) {
            int minTime;
            try {
                minTime = Integer.parseInt(minDuration);
            } catch (NumberFormatException exception) {
                log.error("Unable to parse duration-exceeds attribute: {}, assuming 1000 (1 second)", minDuration, exception);
                minTime = 1000;
            }
            this.minTime = minTime;
        }

        String exceptionOnlyVal = e.getAttribute("exception-only");
        exceptionOnly = Boolean.parseBoolean(exceptionOnlyVal);
        String exceptionVal = e.getAttribute("exception");
        this.exception = Boolean.parseBoolean(exceptionVal);
        // When user specifies "duration-exceeds=20ms", we assume we need to
        if (exceptionOnlyVal.length() == 0 && exceptionVal.length() == 0 && minTime != Integer.MAX_VALUE) {
            this.exception = true;
        }

        return this;
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        delegate.declareLocals(ma);
        if (minTime == Integer.MAX_VALUE) return;
        int startTime = ma.newLocal(Type.INT_TYPE);
        ma.setStartTimeVariableNumber(startTime);
        ma.getIntTime();
        ma.storeLocal(startTime);
    }

    private Label verifyExecutionDuration(ProfileMethodAdapter ma) {
        if (minTime == Integer.MAX_VALUE) {
            return null;
        }
        ma.getIntTime();
        ma.loadLocal(ma.getStartTimeVariableNumber());
        ma.math(GeneratorAdapter.SUB, Type.INT_TYPE);
        ma.push(minTime);
        Label skipExecute = ma.newLabel();
        ma.ifICmp(GeneratorAdapter.LT, skipExecute);
        return skipExecute;
    }

    @Override
    public void onMethodExit(ProfileMethodAdapter ma) {
        if (exceptionOnly)
            return;
        Label skipExecute = verifyExecutionDuration(ma);
        delegate.onMethodEnter(ma);
        delegate.onMethodExit(ma);
        if (skipExecute != null) {
            ma.visitLabel(skipExecute);
            // We are right before *RETURN instruction, thus resulting value is on stack
            // To support java 1.7, we create stackmap frame with arguments as locals and "return value" on stack
            Type returnType = ma.getReturnType();
            if (Type.VOID_TYPE.equals(returnType)) {
                if(delegate.getClass() == ExecuteMethodAfter.class && ((ExecuteMethodAfter) delegate).shouldAddPlainTryCatchBlocks(ma.getClassVersion())) {
                    //Do nothing
                    //Stackmap frame was already added by TryCatch wrapper in delegate
                } else {
                    // numLocal=0 ==> we do not care of the existing variables, and ASM would add "newLocal-added" ones anyway
                    ma.visitFrame(Opcodes.F_NEW, 0, null, 0, EMPTY_STACK);
                }
            } else {
                Object[] stack = new Object[]{TypeUtils.typeToFrameType(returnType)};
                // numLocal=0 ==> we do not care of the existing variables, and ASM would add "newLocal-added" ones anyway
                ma.visitFrame(Opcodes.F_NEW, 0, null, stack.length, stack);
            }
        }
    }

    @Override
    public void onMethodException(ProfileMethodAdapter ma) {
        if (!exception && !exceptionOnly) return;
        Label skipExecute = verifyExecutionDuration(ma);
        delegate.onMethodEnter(ma);
        delegate.onMethodException(ma);
        if (skipExecute != null) {
            ma.visitLabel(skipExecute);
            if(delegate.getClass() == ExecuteMethodAfter.class && ((ExecuteMethodAfter) delegate).shouldAddPlainTryCatchBlocks(ma.getClassVersion())) {
                //Do nothing
                //Stackmap frame was already added by TryCatch wrapper in delegate
            } else {
                ma.visitFrame(Opcodes.F_NEW, 0, null, 0, null);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GuardedAction)) return false;
        if (!super.equals(o)) return false;

        GuardedAction that = (GuardedAction) o;

        if (minTime != that.minTime) return false;
        if (exception != that.exception) return false;
        if (exceptionOnly != that.exceptionOnly) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + minTime;
        result = 31 * result + (exception ? 1 : 0);
        result = 31 * result + (exceptionOnly ? 1 : 0);
        return result;
    }
}
