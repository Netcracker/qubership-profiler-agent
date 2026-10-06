package com.netcracker.profiler.io;

import com.netcracker.profiler.agent.InflightCall;
import com.netcracker.profiler.agent.InflightCall_01;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.jspecify.annotations.Nullable;

import java.io.File;

public class ParamReaderFromMemory_02 extends ParamReaderFromMemory_01 {
    @AssistedInject
    public ParamReaderFromMemory_02(@Assisted("root") @Nullable File root) {
        super(root);
        InflightCall_01.class.getName();
    }

    @Override
    protected Call convert(InflightCall call) {
        Call dst = super.convert(call);
        InflightCall_01 src = (InflightCall_01) call;
        dst.fileRead = src.fileRead();
        dst.fileWritten = src.fileWritten();
        dst.netRead = src.netRead();
        dst.netWritten = src.netWritten();
        return dst;
    }
}
