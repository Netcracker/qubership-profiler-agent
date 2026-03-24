package com.netcracker.profiler.model;

public record ParameterInfoDto(String name, boolean index, boolean list, String signatureFunction, int order) {
    @Override
    public String toString() {
        return "ParameterInfoDto{" +
                "name='" + name + '\'' +
                ", index=" + index +
                ", list=" + list +
                ", signatureFunction='" + signatureFunction + '\'' +
                ", order=" + order +
                '}';
    }
}
