package com.netcracker.common.models;

public sealed interface IStreamType permits StreamType {

    String getName();

    boolean isRotationRequired();
}
