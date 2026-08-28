package com.netcracker.profiler.io.listener;

import com.netcracker.profiler.dump.DumpFile;


/**
 * @author logunov
 */
public interface FileRotatedListener {
    void fileRotated(DumpFile oldFile, DumpFile newFile);
}
