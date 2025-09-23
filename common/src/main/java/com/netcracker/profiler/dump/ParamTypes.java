package com.netcracker.profiler.dump;

public class ParamTypes {
    public final static int IS_BIG = 1;
    public final static int DEDUPLICATE = 2;
    public final static int INDEX = 2;

    public final static int PARAM_INLINE = 0;
    public final static int PARAM_INDEX = INDEX;
    public final static int PARAM_BIG = IS_BIG;
    public final static int PARAM_BIG_DEDUP = IS_BIG|DEDUPLICATE;

    public final static int PARAM_REACTOR = 6;
}
