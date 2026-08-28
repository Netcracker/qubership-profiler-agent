package com.netcracker.profiler.fetch;

import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.stack.DumpVisitor;
import com.netcracker.profiler.sax.stack.DumpsVisitor;
import com.netcracker.profiler.threaddump.parser.MethodThreadLineInfo;
import com.netcracker.profiler.threaddump.parser.ThreadFormatParser;
import com.netcracker.profiler.threaddump.parser.ThreadInfo;
import com.netcracker.profiler.threaddump.parser.ThreaddumpParser;
import com.netcracker.profiler.util.IOHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class StackcollapseParser implements ThreadFormatParser {
    private final DumpsVisitor dumps;

    public StackcollapseParser(DumpsVisitor dumps) {
        this.dumps = dumps;
    }

    public void parse(Reader input, String name) {
        DumpVisitor dv = dumps.visitDump();
        BufferedReader br = IOHelper.ensureBuffered(input);
        String s = null;
        try {
            while ((s = br.readLine()) != null) {
                // Example input:  swapper;start_kernel;rest_init;cpu_idle;default_idle;native_safe_halt 1
                ThreadInfo thread = parseThread(s);
                dv.visitThread(thread);
            }
        } catch (Error e) {
            throw e;
        } catch (IOException e) {
            ErrorSupervisor.getInstance().warn("Error while parsing stackcollapse dump " + name + " at line " + s, e);
        } finally {
            dv.visitEnd();
        }

    }

    public ThreadInfo parseThread(String s) {
        String[] methods = s.split(";");
        String lastMethod = methods[methods.length - 1];
        int lastSpace = lastMethod.lastIndexOf(' ');
        int count = 1;
        if (lastSpace != -1) {
            methods[methods.length - 1] = lastMethod.substring(0, lastSpace);
            count = Integer.parseInt(lastMethod.substring(lastSpace + 1));
        }
        ThreadInfo thread = new ThreadInfo();
        thread.value = count;
        for (int i = methods.length-1; i >=0; i--) {
            String method = methods[i];
            ThreaddumpParser.ThreadLineInfo frame = parseThreadLine(method);
            thread.addThreadLine(frame);
        }
        return thread;
    }

    public ThreaddumpParser.ThreadLineInfo parseThreadLine(String method) {
        MethodThreadLineInfo frame = new MethodThreadLineInfo();

        int lastDot = method.indexOf('.');
        if (lastDot != -1) {
            // Java, sample input: java/util/concurrent/ThreadPoolExecutor.runWorker
            frame.methodName = method.substring(lastDot + 1);
            frame.className = method.substring(0, lastDot).replace('/', '.');
            frame.locationClass = "Unknown";
            return frame;
        }
        // Assume C/C++
        // function name, following a precedent by some tools (Linux perf's _[k]):
        //   _[k] for kernel
        //   _[i] for inlined
        //   _[j] for jit
        //   _[w] for waker
        // Sample inputs:
        //   MutableSpace::free_in_words() const
        //   pthread_mutex_trylock
        //   run_timer_softirq_[k]
        //   CollectedHeap::fill_with_object(HeapWord*, unsigned long, bool)
        //   vmxnet3_poll_rx_only     [vmxnet3]_[k]
        //   pthread_cond_wait@@GLIBC_2.3.2
        int argStart = method.indexOf('(');
        if (argStart != -1) {
            int argEnd = method.lastIndexOf(')');
            if (argEnd == -1) {
                argEnd = method.length();
            }
            frame.arguments = method.substring(argStart + 1, argEnd);
            method = method.substring(0, argStart);
        }
        // Parse pthread_cond_wait@@GLIBC_2.3.2
        String libName = null;
        int doubleAt = method.indexOf("@@");
        if (doubleAt != -1) {
            libName = method.substring(doubleAt + 2).replace('.', '_');
            method = method.substring(0, doubleAt);
        }
        // Parse MutableSpace::free_in_words() const
        int doubleColon = method.indexOf("::");
        if (doubleColon != -1) {
            frame.className = method.substring(0, doubleColon);
            frame.methodName = method.substring(doubleColon + 2);
        } else {
            frame.methodName = method;
        }
        frame.methodName = frame.methodName.replace(" ", ""); // trim spaces
        frame.className = prependPackage(frame.className, libName);

        if (frame.methodName.endsWith("_[k]")) {
            frame.methodName = frame.methodName.substring(0, frame.methodName.length()-4);
            frame.className = prependPackage(frame.className, "kernel");
        }
        if (frame.className == null) {
            frame.className = "native";
        }
        return frame;
    }

    private static String prependPackage(String dst, String packageName) {
        if (dst == null || dst.length() == 0) {
            return packageName;
        }
        if (packageName == null || packageName.length() == 0) {
            return dst;
        }
        return packageName + "." + dst;
    }
}
