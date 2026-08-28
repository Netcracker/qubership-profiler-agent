package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;

import java.util.List;

public interface IParamsStreamVisitor {

    void visitParam(String name, boolean index, boolean list, int order, String signature);

    List<ParameterInfoDto> getAndCleanParams();
}
