package org.qubership.profiler.io.listener;

import org.qubership.profiler.dump.DumpFile;


/**
 * @author logunov
 */
public interface FileRotatedListener {
    void fileRotated(DumpFile oldFile, DumpFile newFile);
}
