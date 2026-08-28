package com.netcracker.profiler.io;

import com.netcracker.profiler.agent.InflightCall;
import com.netcracker.profiler.agent.InflightCall_02;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.jspecify.annotations.Nullable;

import java.io.File;

public class ParamReaderFromMemory_03 extends ParamReaderFromMemory_02 {
    @AssistedInject
    public ParamReaderFromMemory_03(@Assisted("root") @Nullable File root) {
        super(root);
        InflightCall_02.class.getName();
    }

    @Override
    protected Call convert(InflightCall call) {
        Call dst = super.convert(call);
        InflightCall_02 src = (InflightCall_02) call;
        dst.transactions = src.transactions();
        dst.queueWaitDuration = src.queueWaitDuration();
        return dst;
    }
}
