package com.netcracker.profiler.io;

import com.google.inject.assistedinject.Assisted;
import org.jspecify.annotations.Nullable;

import java.util.Set;

/**
 * Factory interface for creating CallReaderFile instances with runtime parameters.
 */
public interface CallReaderFileFactory {
    CallReaderFile create(
            @Assisted("callback") CallListener callback,
            @Assisted("filterer") CallFilterer cf,
            @Nullable @Assisted("nodes") Set<String> nodes,
            @Assisted("readDictionary") boolean readDictionary,
            @Nullable @Assisted("dumpDirs") Set<String> dumpDirs
    );
}
