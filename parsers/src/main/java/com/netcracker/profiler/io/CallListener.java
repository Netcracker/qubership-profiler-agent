package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public interface CallListener {
    void processCalls(String rootReference, ArrayList<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds);
}
