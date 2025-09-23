package com.netcracker.profiler.io.exceptions;

import com.netcracker.profiler.sax.raw.LoggingSupervisor;

import java.util.ArrayList;

public abstract class ErrorSupervisor {
    private final static ThreadLocal<ArrayList<ErrorSupervisor>> SUPERVISORS = new ThreadLocal<ArrayList<ErrorSupervisor>>() {
        @Override
        protected ArrayList<ErrorSupervisor> initialValue() {
            ArrayList<ErrorSupervisor> state = new ArrayList<ErrorSupervisor>();
            state.add(LoggingSupervisor.INSTANCE); // By default log all warnings/errors
            return state;
        }
    };

    public static ErrorSupervisor getInstance() {
        ArrayList<ErrorSupervisor> list = SUPERVISORS.get();
        return list.get(list.size() - 1);
    }

    public static void push(ErrorSupervisor next) {
        if (next == null) {
            throw new IllegalArgumentException("ErrorSupervisor must not be null");
        }
        SUPERVISORS.get().add(next);
    }

    public static void pop() {
        ArrayList<ErrorSupervisor> list = SUPERVISORS.get();
        if (list.size() == 1) {
            throw new IllegalStateException("Cannot remove the last supervisor");
        }
        list.remove(list.size() - 1);
    }

    public static <T extends ErrorSupervisor> T findFirst(Class<T> target) {
        ArrayList<ErrorSupervisor> list = SUPERVISORS.get();
        for (ErrorSupervisor supervisor : list) {
            if (target.isInstance(supervisor))
                return (T) supervisor;
        }
        return null;
    }

    public abstract void warn(String message, Throwable t);

    public abstract void error(String message, Throwable t);
}
