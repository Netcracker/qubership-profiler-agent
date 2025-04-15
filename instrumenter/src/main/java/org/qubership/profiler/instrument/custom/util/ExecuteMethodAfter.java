package org.qubership.profiler.instrument.custom.util;

import org.qubership.profiler.agent.Configuration_01;
import org.qubership.profiler.instrument.ProfileMethodAdapter;
import org.qubership.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class ExecuteMethodAfter extends ExecuteMethod {
    private static final Logger log = LoggerFactory.getLogger(ExecuteMethodAfter.class);
    private boolean catchesException = false;

    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        catchesException = e.hasAttribute("exception-only") || e.hasAttribute("exception");
        return super.init(e, configuration);
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        super.declareLocals(ma);
        if(ma.getReturnType().getSort() != Type.VOID) {
            ma.declareResultVariable();
        }
    }

    @Override
    protected String parseMethodArgs(String methodName) {
        if (!catchesException)
            return super.parseMethodArgs(methodName);

        int argStart = methodName.indexOf('(');
        String newMethodArg = methodName;

        if (argStart == -1)
            newMethodArg = methodName + "(throwable)";
        else if (methodName.indexOf("throwable", argStart) == -1)
            newMethodArg = methodName.substring(0, methodName.length() - 1) + ",throwable)";

        if (!methodName.equals(newMethodArg)) {
            log.debug("Please specify <<throwable>> argument position in execute-when. Automatically used {} instead of configured {}", newMethodArg, methodName);
        }
        return super.parseMethodArgs(newMethodArg);
    }

    @Override
    public void onMethodExit(ProfileMethodAdapter ma) {
        super.onMethodExit(ma);
        int result = -1;
        if (ma.getReturnType().getSort() != Type.VOID) {
            result = ma.getResultVariableNumber();
            ma.storeLocal(result);
        }
        if(shouldAddPlainTryCatchBlocks(ma.getClassVersion())) {
            TryCatchData tryCatchData = appendTry(ma);
            appendCall(ma);
            appendCatch(ma, null, tryCatchData);
        } else {
            appendCall(ma);
        }
        if(result!=-1){
            ma.loadLocal(result);
        }
    }

    @Override
    public void onMethodException(ProfileMethodAdapter ma) {
        super.onMethodException(ma);
        if(shouldAddPlainTryCatchBlocks(ma.getClassVersion())) {
            TryCatchData tryCatchData = appendTry(ma);
            appendCall(ma);
            appendCatch(ma, null, tryCatchData);
        } else {
            appendCall(ma);
        }
    }
}
