package com.netcracker.profiler.instrument.custom;

import com.netcracker.profiler.instrument.ProfileMethodAdapter;

public interface MethodAcceptor {
    /**
     * Local variables should be declared before try-catch block of profileMethodAdapter.
     * In case newLocal is used in onMethod* methods, the resulting stackmap is likely to be invalid
     *
     * @param ma method adapter that should be used to append bytecode
     */
    public void declareLocals(ProfileMethodAdapter ma);

    /**
     * This method is called when instrumented method begins
     *
     * @param ma method adapter that should be used to append bytecode
     */
    public void onMethodEnter(ProfileMethodAdapter ma);

    /**
     * This method is called only for normal exit from the method.
     *
     * @param ma method adapter that should be used to append bytecode
     */
    public void onMethodExit(ProfileMethodAdapter ma);

    public void onMethodException(ProfileMethodAdapter ma);
}
