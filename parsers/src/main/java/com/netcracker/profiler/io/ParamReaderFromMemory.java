package com.netcracker.profiler.io;

import com.netcracker.profiler.agent.*;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.tags.Dictionary;
import com.netcracker.profiler.tags.DictionaryList;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.jspecify.annotations.Nullable;

import java.io.File;
import java.util.*;

public class ParamReaderFromMemory extends ParamReaderFile {
    @AssistedInject
    public ParamReaderFromMemory(@Assisted("root") @Nullable File root) {
        super(root);
        Configuration_01.class.getName();
        DumperPlugin_01.class.getName();
    }

    @Override
    public Map<String, ParameterInfoDto> fillParamInfo(Collection<Throwable> exceptions, String rootReference) {
        if (canReadFromMemory(root)) {
            final ProfilerTransformerPlugin transformer = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);
            final Configuration_01 conf = (Configuration_01) transformer.getConfiguration();
            Map<String, ParameterInfo> infoMap = conf.getParametersInfo();
            Map<String, ParameterInfoDto> dtoMap = new HashMap<String, ParameterInfoDto>();
            for (Map.Entry<String, ParameterInfo> entry : infoMap.entrySet()) {
                ParameterInfo info = entry.getValue();
                ParameterInfoDto dto = new ParameterInfoDto(info.name);
                dto.big = info.big;
                dto.combined = info.combined;
                dto.deduplicate = info.deduplicate;
                dto.index = info.index;
                dto.list = info.list;
                dto.order = info.order;
                dto.signatureFunction = info.signatureFunction;
                dtoMap.put(entry.getKey(), dto);
            }
            return dtoMap;
        }

        return super.fillParamInfo(exceptions, rootReference);
    }

    @Override
    public Dictionary fillTags(BitSet requiredIds, Collection<Throwable> exceptions) {
        if (canReadFromMemory(root)) {
            return new DictionaryList(ProfilerData.getTags());
        }

        return super.fillTags(requiredIds, exceptions);
    }

    private boolean canReadFromMemory(File root) {
        final DumperPlugin_01 dumper = (DumperPlugin_01) Bootstrap.getPlugin(DumperPlugin.class);
        if (dumper == null) return false;
        final File dumpRoot = dumper.getCurrentRoot();
        return dumpRoot != null && dumpRoot.getAbsolutePath().equals(root.getAbsolutePath());
    }
}
