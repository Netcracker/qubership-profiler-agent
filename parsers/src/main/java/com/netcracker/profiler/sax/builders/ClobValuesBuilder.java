package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.chart.Provider;
import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.raw.ClobValueVisitor;
import com.netcracker.profiler.sax.raw.StrReader;
import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.util.ProfilerConstants;

import java.io.IOException;

public class ClobValuesBuilder extends ClobValueVisitor implements Provider<ClobValues> {
    private final ClobValues cv;
    private final int paramsTrimSize;

    public ClobValuesBuilder(int paramsTrimSize) {
        this(ProfilerConstants.PROFILER_V1, new ClobValues(), paramsTrimSize);
    }

    protected ClobValuesBuilder(int api, ClobValues cv, int paramsTrimSize) {
        super(api);
        this.cv = cv;
        this.paramsTrimSize = paramsTrimSize;
    }

    @Override
    public void acceptValue(ClobValue clob, StrReader chars) {
        try {
            clob.value = chars.subSequence(0, Math.min(chars.length(), paramsTrimSize));
        } catch (IOException e) {
            ErrorSupervisor.getInstance().warn("Unable to read clob " + String.valueOf(clob), e);
        }
        cv.add(clob);
    }

    public ClobValues get() {
        return cv;
    }
}
