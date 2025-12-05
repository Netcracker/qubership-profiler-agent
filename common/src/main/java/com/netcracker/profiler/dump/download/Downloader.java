package com.netcracker.profiler.dump.download;

import com.netcracker.profiler.io.Pair;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

public interface Downloader extends Closeable {
    void connect() throws IOException;

    DownloadProgress downloadAsync(List<Pair<RemoteFile, File>> remoteAndLocalFiles) throws IOException;
}
