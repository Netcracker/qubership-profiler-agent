package com.netcracker.profiler.instrument.custom;

import com.netcracker.profiler.instrument.ProfileClassAdapter;

public interface ClassAcceptor {
    public void onClass(ProfileClassAdapter ca, String className);
}
