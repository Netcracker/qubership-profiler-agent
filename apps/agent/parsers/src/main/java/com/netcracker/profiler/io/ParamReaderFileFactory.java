package com.netcracker.profiler.io;

import com.google.inject.assistedinject.Assisted;
import org.jspecify.annotations.Nullable;

import java.io.File;

/**
 * Factory interface for creating ParamReader instances with runtime parameters.
 * Uses Guice AssistedInject to create ParamReaderFile instances.
 */
public interface ParamReaderFileFactory {
    /**
     * Create a ParamReader instance for the given root file.
     * @param root The root directory File, or null for in-memory
     * @return ParamReader instance
     */
    ParamReaderFile create(@Assisted("root") @Nullable File root);
}
