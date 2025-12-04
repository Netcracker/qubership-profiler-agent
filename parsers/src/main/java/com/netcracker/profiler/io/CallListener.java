package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.tags.Dictionary;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;

public interface CallListener {
    void processCalls(String rootReference, ArrayList<Call> calls, Dictionary tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds);
}
