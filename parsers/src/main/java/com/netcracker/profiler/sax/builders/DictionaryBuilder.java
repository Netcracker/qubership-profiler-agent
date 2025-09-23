package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.chart.Provider;
import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.sax.raw.DictionaryVisitor;
import com.netcracker.profiler.util.ProfilerConstants;

public class DictionaryBuilder extends DictionaryVisitor implements Provider<TagDictionary> {
    private final TagDictionary dict;

    public DictionaryBuilder() {
        this(ProfilerConstants.PROFILER_V1, new TagDictionary());
    }

    protected DictionaryBuilder(int api, TagDictionary dict) {
        super(api);
        this.dict = dict;
    }

    @Override
    public void visitName(int id, String name) {
        dict.put(id, name);
    }

    @Override
    public void visitParamInfo(ParameterInfoDto info) {
        dict.info(info);
    }

    public TagDictionary get() {
        return dict;
    }
}
