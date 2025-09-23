package com.netcracker.profiler.instrument.custom;

import com.netcracker.profiler.instrument.ProfileMethodAdapter;

import java.util.ArrayList;

public class MethodAcceptorsList implements MethodAcceptor {
    ArrayList<MethodAcceptor> list = new ArrayList<MethodAcceptor>();

    public void add(MethodAcceptor ma) {
        list.add(ma);
    }

    public void declareLocals(ProfileMethodAdapter ma) {
        for (MethodAcceptor aList : list) aList.declareLocals(ma);
    }

    public void onMethodEnter(ProfileMethodAdapter ma) {
        for (MethodAcceptor aList : list) aList.onMethodEnter(ma);
    }

    public void onMethodExit(ProfileMethodAdapter ma) {
        for (int i = list.size() - 1; i >= 0; i--) {
            MethodAcceptor aList = list.get(i);
            aList.onMethodExit(ma);
        }
    }

    public void onMethodException(ProfileMethodAdapter ma) {
        for (int i = list.size() - 1; i >= 0; i--) {
            MethodAcceptor aList = list.get(i);
            aList.onMethodException(ma);
        }
    }

    @Override
    public int hashCode() {
        return list != null ? list.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MethodAcceptorsList that = (MethodAcceptorsList) o;

        if (list != null ? !list.equals(that.list) : that.list != null) return false;

        return true;
    }
}
