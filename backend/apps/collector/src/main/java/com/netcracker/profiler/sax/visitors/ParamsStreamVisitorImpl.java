package com.netcracker.profiler.sax.visitors;

import com.netcracker.profiler.model.ParameterInfoDto;

import java.util.ArrayList;
import java.util.List;

public class ParamsStreamVisitorImpl implements IParamsStreamVisitor {
    private List<ParameterInfoDto> params = new ArrayList<>();

    @Override
    public void visitParam(String name, boolean list, boolean index, int order, String signature) {
        params.add(new ParameterInfoDto(name, index, list, signature, order));
    }

    @Override
    public List<ParameterInfoDto> getAndCleanParams() {
        List<ParameterInfoDto> paramsModels = new ArrayList<>(this.params);
        this.params.clear();
        return paramsModels;
    }

}
