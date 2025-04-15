package org.qubership.profiler.instrument.custom.util;


import org.qubership.profiler.instrument.ProfileMethodAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteMethodBefore extends ExecuteMethod {
    public static final Logger log = LoggerFactory.getLogger(ExecuteMethodBefore.class);

    @Override
    public void onMethodEnter(ProfileMethodAdapter ma) {
        super.onMethodEnter(ma);
        Object[] locals = ma.getMethodArgsAsLocals();
        if(shouldAddPlainTryCatchBlocks(ma.getClassVersion())) {
            TryCatchData tryCatchData = appendTry(ma);
            appendCall(ma);
            appendCatch(ma, locals, tryCatchData);
        } else {
            appendCall(ma);
        }
    }
}
