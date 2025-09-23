package com.netcracker.profiler.instrument.custom;

import com.netcracker.profiler.instrument.ProfileClassAdapter;

import java.util.ArrayList;

public class ClassAcceptorsList implements ClassAcceptor {
    ArrayList<ClassAcceptor> list = new ArrayList<ClassAcceptor>();

    public void add(ClassAcceptor ca) {
        list.add(ca);
    }

    public void onClass(ProfileClassAdapter ca, String className) {
        for (ClassAcceptor aList : list) aList.onClass(ca, className);
    }

    @Override
    public int hashCode() {
        return list != null ? list.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClassAcceptorsList that = (ClassAcceptorsList) o;

        if (list != null ? !list.equals(that.list) : that.list != null) return false;

        return true;
    }
}
