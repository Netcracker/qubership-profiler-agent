package org.postgresql.core.v3;

abstract class SimpleParameterList implements V3ParameterList {
    public native int[] getParamTypes();
    public native Object[] getValues();
    public native String toString(int index, boolean standardConformingStrings);
}
