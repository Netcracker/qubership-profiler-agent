package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.util.ProfilerConstants;

public class DictionaryVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final DictionaryVisitor dv;

    public DictionaryVisitor(int api) {
        this(api, null);
    }

    public DictionaryVisitor(int api, DictionaryVisitor dv) {
        this.api = api;
        this.dv = dv;
    }

    public void visitName(int id, String name) {
        if (dv != null)
            dv.visitName(id, name);
    }

    public void visitParamInfo(ParameterInfoDto info) {
        if (dv != null)
            dv.visitParamInfo(info);
    }

    public void visitEnd() {
        if (dv != null)
            dv.visitEnd();
    }
}
