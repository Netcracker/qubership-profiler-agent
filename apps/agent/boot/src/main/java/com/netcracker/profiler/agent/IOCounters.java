package com.netcracker.profiler.agent;

import java.lang.reflect.Field;

public class IOCounters {
    private static final ESCLogger logger = ESCLogger.getLogger(IOCounters.class);
    public static void fileRead(LocalState state, int size) {
        if (size > 0)
            state.fileRead += size;
    }

    public static void fileWritten(LocalState state, byte[] b) {
        if (b != null)
            state.fileWritten += b.length;
    }

    public static void fileWritten(LocalState state, int size) {
        if (size > 0)
            state.fileWritten += size;
    }

    public static void netRead(LocalState state, int size) {
        netRead(state, (long)size);
    }

    public static void netRead(LocalState state, long size) {
        if (size > 0)
            state.netRead += size;
    }

    public static void netWritten(LocalState state, int size) {
        netWritten(state, (long)size);
    }

    public static void netWritten(LocalState state, long size) {
        if (size > 0)
            state.netWritten += size;
    }

    private static boolean inputSocketUnavailable;
    private static boolean outputSocketUnavailable;
    private static Field getOutputSocket;
    private static Field getInputSocket;

    public static void dumpInputSocket(Object os) {
        if (inputSocketUnavailable)
            return;
        if (getInputSocket == null) {
            Field s = getInputSocket = getSocketImpl(os);
            if (s == null) {
                inputSocketUnavailable = true;
                return;
            }
        }
        try {
            Object impl = getInputSocket.get(os);
            Profiler.event(impl.toString(), "socket");
        } catch (Throwable t) {
            /**/
        }
    }

    public static void dumpOutputSocket(Object os) {
        if (outputSocketUnavailable)
            return;
        if (getOutputSocket == null) {
            Field s = getOutputSocket = getSocketImpl(os);
            if (s == null) {
                outputSocketUnavailable = true;
                return;
            }
        }
        try {
            Object impl = getOutputSocket.get(os);
            Profiler.event(impl.toString(), "socket");
        } catch (Throwable t) {
            /**/
        }
    }

    public static void dumpSocket(Object socket) {
        try {
            if (socket != null) {
                Profiler.event(socket.toString(), "socket");
            }
        } catch (Throwable t) {
            /**/
        }
    }

    private static Field getSocketImpl(Object stream) {
        Field impl;
        try {
            impl = stream.getClass().getDeclaredField("impl");
            try {
                impl.setAccessible(true);
            } catch (Throwable t) {
                /**/
            }
        } catch (Throwable e) {
            logger.severe("", e);
            return null;
        }
        return impl;
    }
}
