package com.netcracker.profiler

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import org.openjdk.jmh.profile.GCProfiler
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.OptionsBuilder
import com.netcracker.profiler.agent.Bootstrap
import com.netcracker.profiler.agent.DumperPlugin
import com.netcracker.profiler.agent.Profiler
import com.netcracker.profiler.agent.ProfilerTransformerPlugin
import java.util.concurrent.TimeUnit

@Fork(value = 5, jvmArgsPrepend = ["-Xmx128m", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints"])
@Measurement(iterations = 60, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@Threads(5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
open class LocalBufferBenchmark {
    @Param("101")
    var cpuTokens: Long = 1

    var arg = "test"

    @Setup(Level.Trial)
    fun setup() {
        Bootstrap.registerPlugin(DumperPlugin::class.java, NoopDumperPlugin())
        Bootstrap.registerPlugin(ProfilerTransformerPlugin::class.java, NoopProfilerTransformerPlugin())
    }

    /**
     * Calls several profiled methods and logs the event at the end.
     * Event logging calls the data to be written.
     */
    @Benchmark
    fun enterEnterEvent(bh: Blackhole) {
        // This is a typical structure of the profiled methods
        val localState = Profiler.enterReturning(42);
        try {
            foo(bh)
        } finally {
            localState.exit();
        }
    }

    fun foo(bh: Blackhole) {
        val localState = Profiler.enterReturning(43);
        try {
            bar(bh, arg);
        } finally {
            localState.exit();
        }
    }

    fun bar(bh: Blackhole, arg: String) {
        val localState = Profiler.enterReturning(44);
        localState.event(arg, 45)
        try {
            Blackhole.consumeCPU(cpuTokens);
        } finally {
            localState.exit();
        }
    }

    /**
     * Calls several profiled methods; however, it exists fast, so no dada is logged
     */
    @Benchmark
    fun enterExit(bh: Blackhole) {
        // This is a typical structure of the profiled methods
        val localState = Profiler.enterReturning(42);
        try {
            foo1()
        } finally {
            localState.exit();
        }
    }

    fun foo1() {
        val localState = Profiler.enterReturning(43);
        try {
            bar1();
        } finally {
            localState.exit();
        }
    }

    fun bar1() {
        val localState = Profiler.enterReturning(44);
        try {
            baz1()
        } finally {
            localState.exit();
        }
    }

    fun baz1() {
        val localState = Profiler.enterReturning(44);
        try {
            Blackhole.consumeCPU(cpuTokens);
        } finally {
            localState.exit();
        }
    }

}

fun main() {
    val opt = OptionsBuilder()
        .include(LocalBufferBenchmark::class.java.getSimpleName())
        .addProfiler(GCProfiler::class.java)
        .detectJvmArgs()
        .build()
    Runner(opt).run()
}
