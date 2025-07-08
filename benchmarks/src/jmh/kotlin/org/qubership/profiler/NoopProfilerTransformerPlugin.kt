package org.qubership.profiler

import org.qubership.profiler.agent.Configuration
import org.qubership.profiler.agent.ProfilerTransformerPlugin_01
import org.qubership.profiler.agent.ReloadStatus

/**
 * This is a no-op implementation of ProfilerTransformerPlugin so the profiler does not fail on startup.
 */
class NoopProfilerTransformerPlugin: ProfilerTransformerPlugin_01 {
    override fun getConfiguration(): Configuration {
        TODO("Not yet implemented")
    }

    override fun getReloadStatus(): ReloadStatus {
        TODO("Not yet implemented")
    }

    override fun reloadConfiguration(newConfigPath: String?) {
    }

    override fun reloadClasses(classNames: MutableSet<String>?) {
    }
}
