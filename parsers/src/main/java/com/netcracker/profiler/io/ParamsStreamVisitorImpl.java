package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;

import java.util.ArrayList;
import java.util.List;

public class ParamsStreamVisitorImpl implements IParamsStreamVisitor {
    private String podName;
    private List<ParameterInfoDto> params = new ArrayList<>();

    public ParamsStreamVisitorImpl(String podName) {
        this.podName = podName;
    }

    @Override
    public void visitParam(String name, boolean list, boolean index, int order, String signature) {
        ParameterInfoDto parameterInfoDto = new ParameterInfoDto(name);
        parameterInfoDto.index(index);
        parameterInfoDto.list(list);
        parameterInfoDto.order(order);
        parameterInfoDto.signature(signature);
        params.add(parameterInfoDto);
    }

    @Override
    public List<ParameterInfoDto> getAndCleanParams() {
        List<ParameterInfoDto> paramsModels = new ArrayList<>(this.params);

        this.params.clear();

        return paramsModels;
    }

}
