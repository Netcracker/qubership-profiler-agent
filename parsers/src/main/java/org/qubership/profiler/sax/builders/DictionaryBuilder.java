package org.qubership.profiler.sax.builders;

import org.qubership.profiler.chart.Provider;
import org.qubership.profiler.configuration.ParameterInfoDto;
import org.qubership.profiler.dom.TagDictionary;
import org.qubership.profiler.sax.raw.DictionaryVisitor;
import org.qubership.profiler.util.ProfilerConstants;

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
