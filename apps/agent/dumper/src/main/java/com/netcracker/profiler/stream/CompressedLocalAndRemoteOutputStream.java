package com.netcracker.profiler.stream;

import static com.netcracker.profiler.util.ProfilerConstants.CALL_HEADER_MAGIC;

import com.netcracker.profiler.agent.DumperCollectorClient;
import com.netcracker.profiler.cloud.transport.ProfilerProtocolException;
import com.netcracker.profiler.cloud.transport.ProtocolConst;
import com.netcracker.profiler.dump.DataOutputStreamEx;
import com.netcracker.profiler.dump.DumpFile;
import com.netcracker.profiler.dump.FlushableGZIPOutputStream;
import com.netcracker.profiler.dump.IDataOutputStreamEx;
import com.netcracker.profiler.exception.ProfilerAgentIOException;
import com.netcracker.profiler.io.RemoteAndLocalOutputStream;
import com.netcracker.profiler.io.listener.FileRotatedListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * this one orchestrates rotation of remote streams and local files
 */
public class CompressedLocalAndRemoteOutputStream implements ICompressedLocalAndRemoteOutputStream {
    public static final Logger log = LoggerFactory.getLogger(CompressedLocalAndRemoteOutputStream.class);

    private static boolean isGZIPOutputStreamSyncFlushSupported; //it was implemented in java 7b97

    static {
        try {
            GZIPOutputStream.class.getDeclaredConstructor(OutputStream.class, int.class, boolean.class);
            isGZIPOutputStreamSyncFlushSupported = true;
        } catch (Throwable t) {
            isGZIPOutputStreamSyncFlushSupported = false;
        }
    }

    // Non static since NumberFormat is not thread safe
    final NumberFormat fileIndexFormat = NumberFormat.getIntegerInstance();

    {
        fileIndexFormat.setGroupingUsed(false);
        fileIndexFormat.setMinimumIntegerDigits(6);
    }

    private File root;
    private final String name;
    private RemoteAndLocalOutputStream remote;
    //so that this stream can follow another stream's sequence during rotation
    private ICompressedLocalAndRemoteOutputStream sequenceSource;

    private int rotateThreshold;

    private long uncompressedSize;
    private long compressedSize;
    private DumperCollectorClient client;
    private boolean rotateForRemote = false;
    private long lastRotatedMillis;

    @Override
    public void setLocalDumpEnabled(boolean localDumpEnabled) {
        this.localDumpEnabled = localDumpEnabled;
    }

    private boolean localDumpEnabled;

    int index;
    DataOutputStreamEx stream;
    File currentFile;

    int version;

    private ICompressedLocalAndRemoteOutputStream dependentStream;
    private List<FileRotatedListener> fileRotatedListeners;

    private Object state;

    public CompressedLocalAndRemoteOutputStream(String name, int rotateThreshold, int version) {
        this(name, rotateThreshold, version, null);
    }

    public CompressedLocalAndRemoteOutputStream(String name, int rotateThreshold, int version, Object state) {
        this.rotateThreshold = rotateThreshold;
        this.name = name;
        this.version = version;
        this.state = state;
    }

    @Override
    public void askRotateForRemote() {
        rotateForRemote = true;
    }

    protected boolean resetExistingContents(){
        return false;
    }

    private RemoteAndLocalOutputStream getRemote(int requestedRollingSequenceId) throws IOException {
        RemoteAndLocalOutputStream stream = new RemoteAndLocalOutputStream(
                client,
                name,
                requestedRollingSequenceId,
                localDumpEnabled,
                resetExistingContents());
        rotateForRemote = false;
        return stream;
    }

    @Override
    public IDataOutputStreamEx getStream() {
        return stream;
    }

    @Override
    public void writePhrase() throws IOException {
        if(remote != null) {
            remote.writePhrase();
        }
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public ICompressedLocalAndRemoteOutputStream setRoot(File root) {
        this.root = root;
        initialize();
        return this;
    }

    protected void initialize() {
        index = sequenceSource == null? 0 : sequenceSource.getIndex();
    }

    @Override
    public void setClient(DumperCollectorClient client) {
        this.client = client;
    }

    @Override
    public CompressedLocalAndRemoteOutputStream rotate() throws IOException {
        index = sequenceSource == null? index + 1 : sequenceSource.getIndex();
        stream = rotateStream();
        if (version != 0)
            stream.writeLong((((long) CALL_HEADER_MAGIC) << 32) | version);
        fileRotated();
        return this;
    }

    @Override
    public void fileRotated() throws IOException {
    }

    private DataOutputStreamEx rotateStream() throws IOException {
        OutputStream result = null;
        File oldFile = currentFile;
        close();

        if (client != null) {
            int requestedRollingSequenceId = index-1;
            remote = getRemote(requestedRollingSequenceId);
            int indexFromRemote = remote.getRollingSequenceId()+1;
            log.debug("Rotated stream {}. New Local index: {}, new remote index: {}", name, index, indexFromRemote);
            //+1 here is for compatibility. indexes start with 1 since rotate() is called right after stream is initialized
            if(sequenceSource != null && sequenceSource.getIndex() != indexFromRemote){
                throw new ProfilerProtocolException("Failed to align sequences of stream " + name + " and its parent stream " + sequenceSource.getName());
            }
            index = indexFromRemote;
            result = remote;
            log.debug("Created dump buffers for local and remote for {} / {}",
                    name, index);
        }
        if (localDumpEnabled) {
            String rollingSequenceId = fileIndexFormat.format(index);
            String fileName = name + File.separatorChar + rollingSequenceId + ".gz";
            log.debug("Opening new {} file", fileName);
            File newFile = new File(root, fileName);
            final File parentFile = newFile.getParentFile();
            if (!parentFile.exists()) {
                log.debug("Creating directory {}", parentFile.getAbsolutePath());
                if (!parentFile.mkdirs())
                    log.error("Unable to create directory {}", parentFile.getAbsolutePath());
            }
            currentFile = newFile;
            notifyFileRotated(oldFile, newFile, (dependentStream == null ? null : dependentStream.getCurrentFile()));

            OutputStream local;
            if(isGZIPOutputStreamSyncFlushSupported) {
                local = new GZIPOutputStream(new FileOutputStream(newFile), ProtocolConst.DATA_BUFFER_SIZE, true);
            } else {
                local = new FlushableGZIPOutputStream(new FileOutputStream(newFile), ProtocolConst.DATA_BUFFER_SIZE);
            }
            if(remote == null) {
                result = local;   //flushableGZIPOutputStream is itself buffered
                log.debug("Skipped remote collector stream creation, local buffer size {}", ProtocolConst.DATA_BUFFER_SIZE);
            } else {
                remote.setLocal(local);
            }
        }
        if (result == null) {
            throw new ProfilerAgentIOException("Cannot write anywhere, both local and remote dumps are disabled.");
        }

        lastRotatedMillis = System.currentTimeMillis();
        return new DataOutputStreamEx(result);
    }

    private void notifyFileRotated(File oldFile, File newFile, File dependentFile) {
        if (fileRotatedListeners == null) {
            return;//nothing to invoke
        }
        for (FileRotatedListener listener : fileRotatedListeners) {
            DumpFile dependentDF = dependentFile == null ? null : new DumpFile(dependentFile.getPath(), -1, -1);
            DumpFile oldDF = oldFile == null ? null : new DumpFile(oldFile.getPath(), oldFile.length(), oldFile.lastModified(), dependentDF);
            DumpFile newDF = newFile == null ? null : new DumpFile(newFile.getPath(), newFile.length(), newFile.lastModified());
            listener.fileRotated(oldDF, newDF);
        }
    }

    @Override
    public void close() throws IOException {
        if (stream == null) return;
        try {
            stream.close();
        } catch(Exception e) {
            log.error("Failed to close previous stream " + name + " during rotation. Will continue rotation anyway", e);
        }
        uncompressedSize += stream.size();
        compressedSize += localDumpEnabled ? currentFile.length() : 1;
        // TODO should also count somehow for remote?
        stream = null;
        currentFile = null;
        return;
    }

    private boolean rotationPeriodPassed(){
        if(remote == null) {
            return false;
        }
        long rotationPeriod = remote.getRotationPeriod();
        long sinceLastRotation = System.currentTimeMillis() - lastRotatedMillis;
        return rotationPeriod > 0 && sinceLastRotation > rotationPeriod;
    }

    /**
     * Checks if current size exceeds limit and rotates stream
     *
     * @return true in case the stream was rotated
     * @throws IOException
     */
    @Override
    public boolean rotateIfRequired() throws IOException {
        boolean rotationPeriodPassed = rotationPeriodPassed();

        long rotationSizeThreshold = Math.min(
                rotateThreshold <= 0 ? Long.MAX_VALUE : rotateThreshold,
                remote == null || remote.getRequiredRotationSize() <= 0 ? Long.MAX_VALUE: remote.getRequiredRotationSize()
        );

        boolean rotationBySizeRequired =
                rotationSizeThreshold != Long.MAX_VALUE &&
                stream != null &&
                stream.size() > rotationSizeThreshold;

        if (!rotateForRemote && !rotationPeriodPassed && !rotationBySizeRequired ) {
            return false;
        }
        log.debug("Rotating stream {}. Size {}. size threshold {}. Rotation by size {}, rotation by time {}. for remote {}",
                name,
                stream == null? null: stream.size(),
                rotationSizeThreshold,
                rotationBySizeRequired,
                rotationPeriodPassed,
                rotateForRemote
        );
        rotate();
        return true;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns uncompressed stream size, including all previous rotations
     *
     * @return uncompressed stream size, including all previous rotations
     */
    @Override
    public long getUncompressedSize() {
        return uncompressedSize + (stream == null ? 0 : stream.size());
    }

    /**
     * Returns compressed stream size, including all previous rotations
     * The compression rate for current file is estimated as 20x, to eliminate filesystem operation .length()
     *
     * @return compressed stream size, including all previous rotations
     */
    @Override
    public long getCompressedSize() {
        return compressedSize + (stream == null ? 0 : stream.size() / 20);
    }

    @Override
    public void addListener(FileRotatedListener listener) {
        if (fileRotatedListeners == null) {
            fileRotatedListeners = new ArrayList<FileRotatedListener>();
        }

        fileRotatedListeners.add(listener);
    }

    @Override
    public void clearListeners() {
        if (fileRotatedListeners != null) {
            fileRotatedListeners.clear();
        }
    }

    @Override
    public Collection<FileRotatedListener> getListeners() {
        return Collections.unmodifiableCollection(fileRotatedListeners);
    }

    public File getCurrentFile() {
        return currentFile;
    }
    public ICompressedLocalAndRemoteOutputStream getDependentStream() {
        return dependentStream;
    }

    public void setDependentStream(ICompressedLocalAndRemoteOutputStream dependentStream) {
        this.dependentStream = dependentStream;
    }

    @Override
    public Object getState() {
        return state;
    }

    @Override
    public void setState(Object state) {
        this.state = state;
    }
}
