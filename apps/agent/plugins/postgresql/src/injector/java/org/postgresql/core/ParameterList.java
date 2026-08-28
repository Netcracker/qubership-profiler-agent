package org.postgresql.core;

public interface ParameterList {
    int getParameterCount();

    String toString(int index);
}
