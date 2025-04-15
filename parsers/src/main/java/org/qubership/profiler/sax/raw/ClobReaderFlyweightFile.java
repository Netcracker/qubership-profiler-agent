package org.qubership.profiler.sax.raw;

import org.qubership.profiler.dump.DataInputStreamEx;

import java.io.File;
import java.io.IOException;

public class ClobReaderFlyweightFile extends AbstractClobReaderFlyweight {
    private File dataFolder;

    public void setDataFolder(File dataFolder) {
        this.dataFolder = dataFolder;
    }

    protected DataInputStreamEx reopenDataInputStream(DataInputStreamEx oldOne, String folder, int fileIndex) throws IOException {
        return DataInputStreamEx.reopenDataInputStream(oldOne, dataFolder, folder, fileIndex);
    }
}
