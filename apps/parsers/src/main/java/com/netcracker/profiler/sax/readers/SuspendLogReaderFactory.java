package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.sax.raw.SuspendLogVisitor;

import com.google.inject.assistedinject.Assisted;

/**
 * Factory interface for creating SuspendLogReader instances.
 */
public interface SuspendLogReaderFactory {
    SuspendLogReader create(
            @Assisted("sv") SuspendLogVisitor sv,
            @Assisted("dataFolderPath") String dataFolderPath);

    SuspendLogReader create(
            @Assisted("sv") SuspendLogVisitor sv,
            @Assisted("dataFolderPath") String dataFolderPath,
            @Assisted("begin") long begin,
            @Assisted("end") long end);
}
