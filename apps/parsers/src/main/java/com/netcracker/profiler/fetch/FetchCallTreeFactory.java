package com.netcracker.profiler.fetch;

import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.CallRowid;

import com.google.inject.assistedinject.Assisted;

/**
 * Factory interface for creating FetchCallTree instances.
 * Guice's AssistedInject will auto-generate the implementation via FactoryModuleBuilder in ParsersModule.
 */
public interface FetchCallTreeFactory {
    FetchCallTree create(
        @Assisted("sv") ProfiledTreeStreamVisitor sv,
        @Assisted("callIds") CallRowid[] callIds,
        @Assisted("paramsTrimSize") int paramsTrimSize
    );

    FetchCallTree create(
        @Assisted("sv") ProfiledTreeStreamVisitor sv,
        @Assisted("callIds") CallRowid[] callIds,
        @Assisted("paramsTrimSize") int paramsTrimSize,
        @Assisted("begin") long begin,
        @Assisted("end") long end
    );
}
