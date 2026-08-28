package com.netcracker.profiler.stream;

import com.netcracker.profiler.agent.DumperCollectorClient;
import com.netcracker.profiler.dump.IDataOutputStreamEx;
import com.netcracker.profiler.io.listener.FileRotatedListener;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

public interface ICompressedLocalAndRemoteOutputStream extends Closeable {

    void setLocalDumpEnabled(boolean localDumpEnabled);

    void askRotateForRemote();

    IDataOutputStreamEx getStream();

    void writePhrase() throws IOException;

    int getIndex();

    ICompressedLocalAndRemoteOutputStream setRoot(File root);

    void setClient(DumperCollectorClient client);

    ICompressedLocalAndRemoteOutputStream rotate() throws IOException;

    void fileRotated() throws IOException;

    boolean rotateIfRequired() throws IOException;

    String getName();

    long getUncompressedSize();

    long getCompressedSize();

    void addListener(FileRotatedListener listener);

    void clearListeners();

    Collection<FileRotatedListener> getListeners();

    File getCurrentFile();
    ICompressedLocalAndRemoteOutputStream getDependentStream();

    void setDependentStream(ICompressedLocalAndRemoteOutputStream dependentStream);

    Object getState();
    void setState(Object state);
}
