package com.netcracker.profiler.sax.visitors;

import com.netcracker.profiler.model.ParameterInfoDto;

import java.util.List;

public interface IParamsStreamVisitor {

    void visitParam(String name, boolean index, boolean list, int order, String signature);

    List<ParameterInfoDto> getAndCleanParams();
}
