package com.netcracker.profiler.sax.string;

import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.sax.parsers.DictionaryParser;
import com.netcracker.profiler.sax.raw.DictionaryVisitor;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.sax.raw.TraceVisitor;
import com.netcracker.profiler.util.ProfilerConstants;

public class StringRepositoryAdapter extends RepositoryVisitor {
    private final TagDictionary dict = new TagDictionary(100);

    public StringRepositoryAdapter(RepositoryVisitor rv) {
        this(ProfilerConstants.PROFILER_V1, rv);
    }

    protected StringRepositoryAdapter(int api, RepositoryVisitor rv) {
        super(api, rv);
    }

    @Override
    public StringTraceAdapter visitTrace() {
        TraceVisitor tv = super.visitTrace();
        if (tv == null)
            return null;
        return new StringTraceAdapter(this, tv);
    }

    @Override
    public void visitEnd() {
        DictionaryVisitor dv = visitDictionary();
        DictionaryParser dp = new DictionaryParser();
        dp.parse(dv, dict);
        super.visitEnd();
    }

    public int allocateId(String id) {
        return dict.resolve(id);
    }

    public StringRepositoryAdapter asSkipVisitEnd() {
        return new StringRepositoryAdapter(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public StringRepositoryAdapter asSkipVisitEnd() {
                return this;
            }
        };
    }
}
