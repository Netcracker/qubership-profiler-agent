package com.netcracker.profiler.dump;

import com.netcracker.profiler.agent.StringUtils;
import com.netcracker.profiler.util.IOHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Possible log file formats:
 * <table>
 *     <caption>Log file formats</caption>
 *     <tr><td>"LOGFORMAT2"</td>
 *     <td>
 *         <ol>
 *             <li>(String) {@code operation}: ['A'|'D']</li>
 *             <li>(String) {@code relative_filepath}: dump file path relative to this log file</li>
 *             <li>(long) {@code timestamp}: dump file last modification date</li>
 *             <li>(long) {@code size}: dump file size</li>
 *             <li>(String) {@code dependentFile}: relative path to dependent dump file</li>
 *         </ol>
 *     </td>
 * </table>
 */
public class DumpFileLog implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DumpFileLog.class);
    public static final String CURRENT_LOG_FORMAT = "LOGFORMAT3";

    private static enum Operation {
        ADD("A"),
        DELETE("D");

        final String name;

        Operation(String name) {
            this.name = name;
        }
    }
    public static final String DEFAULT_NAME = "filelist.blst";

    private final File fileList;

    private DataOutputStreamEx outputStream;
    private boolean justCreated;
    private int logEntryCount, deleteEntryCount;
    private boolean hasDeletes;

    public DumpFileLog(File fileList) {
        this.fileList = fileList;
        this.logEntryCount = 0;
        this.deleteEntryCount = 0;

        if (!this.fileList.exists()) {
            try {
                createFile();
                justCreated = true;
            } catch (IOException e) {
                log.warn(String.format("Can't create log for dump files by path %s", this.fileList.getAbsolutePath()), e);
            }
        }
        if (!this.fileList.exists()) {
            // We tried to create it but failed
            return;
        }
        initWriter();
    }

    private void initWriter() {
        try {
            outputStream = new DataOutputStreamEx(new BufferedOutputStream(new FileOutputStream(this.fileList, true), 65536));
        } catch (IOException e) {
            log.warn(String.format("Error during opening output stream for dump files log '%s'", this.fileList), e);
        }
    }

    private void createFile() throws IOException {
        File parent = fileList.getParentFile();
        if (!parent.exists()) {
            log.warn("Directory {} is absent. Create it", parent);
            parent.mkdirs();
        }
        boolean fileCreated = fileList.createNewFile();
        DataOutputStreamEx out = null;
        try {
            out = new DataOutputStreamEx(new BufferedOutputStream(new FileOutputStream(fileList), 65536));
            writeHeader(out);
        } finally {
            IOHelper.close(out);
        }
    }

    private void writeHeader(DataOutputStreamEx outputStream) throws IOException {
        outputStream.write(CURRENT_LOG_FORMAT);
        outputStream.flush();
    }

    public Queue<DumpFile> parseIfPresent() {
        if (justCreated) {
            log.info("File with dump files log {} has been just created", fileList);
            return null;
        }
        Queue<DumpFile> result = readDumpFileLog();
        cleanup(result);
        return result;
    }

    private Queue<DumpFile> readDumpFileLog() {
        Queue<DumpFile> result = null;
        DataInputStreamEx inputStream = null;
        try {
            inputStream = DataInputStreamEx.openDataInputStream(fileList);
            String formatVersion = inputStream.readString();
            if (!CURRENT_LOG_FORMAT.equals(formatVersion)) {
                throw new IllegalArgumentException(String.format("File format '%s' is unknown", formatVersion));
            }
            // Next we assume that file has the required format

            Queue<DumpFile> dumpFiles = new LinkedList<DumpFile>();
            Set<DumpFile> deletedFiles = new HashSet<DumpFile>();
            String dumpRootDir = fileList.getParent();
            try {
                while (true) {
                    String operation = inputStream.readString(1);
                    String relativeFilePath = inputStream.readString(2000);
                    long timestamp = inputStream.readVarLong();
                    long fileSize = inputStream.readVarLong();
                    String relativeDependentFilePath = inputStream.readString(2000);
                    DumpFile dependentDumpFile = null;
                    if(!StringUtils.isBlank(relativeDependentFilePath)) {
                        dependentDumpFile = new DumpFile(new File(dumpRootDir, relativeDependentFilePath).getPath(), -1, -1);
                    }
                    DumpFile dumpFile = new DumpFile(new File(dumpRootDir, relativeFilePath).getPath(), fileSize, timestamp, dependentDumpFile);
                    // store all files. Filter later
                    if (Operation.DELETE.name.equals(operation)) {
                        deletedFiles.add(dumpFile);
                    } else {
                        dumpFiles.add(dumpFile);
                    }
                }
            } catch (EOFException e) {
                log.info("Read {} entries from file {}", dumpFiles.size(), fileList);
                // ignore
            } catch (OutOfMemoryError e) {
                // ignore
                log.warn("Got OutOfMemoryError while parsing " + fileList.getAbsolutePath() + ". Assuming the file is corrupted. Will rescan dump folder and write new index file.", e);
                return null;
            }

            hasDeletes = !deletedFiles.isEmpty();
            dumpFiles.removeAll(deletedFiles);
            result = dumpFiles;
        } catch (IOException e) {
            log.warn("Can't parse file {}. Will res", fileList, e);
        } finally {
            IOHelper.close(inputStream);
        }
        return result;
    }

    /**
     * Will write (<i>not append</i>) given {@link DumpFile}s to the log file
     * @param dumpFiles {@link java.util.Queue} of {@link DumpFile} to be written.
     *                  <br/> If {@code null} then log file will be erased and filled only with header
     */
    public synchronized void cleanup(Queue<DumpFile> dumpFiles, boolean force) {
        if (!hasDeletes && !force) {
            // No need to rewrite the file
            return;
        }
        close();
        DataOutputStreamEx out;
        try {
            out = new DataOutputStreamEx(new BufferedOutputStream(new FileOutputStream(this.fileList), 65536));
            outputStream = out;
            writeHeader(out);
            logEntryCount = 0;
            deleteEntryCount = 0;
            if (dumpFiles != null) {
                for (DumpFile dumpFile : dumpFiles) {
                    writeOperation(dumpFile, Operation.ADD.name, out);
                }
            }
            hasDeletes = false;
        } catch (IOException e) {
            log.warn("Error during file dump list log cleanup", e);
        }
    }

    public void cleanup(Queue<DumpFile> dumpFiles) {
        cleanup(dumpFiles, false);
    }

    public void writeAddition(DumpFile file) {
        log.debug("Write addition of dump file {}", file);
        String operation = Operation.ADD.name;
        writeOperation(file, operation, outputStream);
    }

    public void writeDeletion(DumpFile file) {
        log.debug("Write deletion of dump file {}", file);
        String operation = Operation.DELETE.name;
        writeOperation(file, operation, outputStream);
        hasDeletes = true;

        deleteEntryCount++;
        if (deleteEntryCount > logEntryCount / 2) {
            log.info("Amount of delete entries in log is {} and it is more than half of total amount of entries {}." +
                    " Will cleanup log"
                    , deleteEntryCount, logEntryCount);
            synchronized (this) {
                Queue<DumpFile> result = readDumpFileLog();
                cleanup(result);
            }
        }
    }

    private synchronized void writeOperation(DumpFile file, String operation, DataOutputStreamEx printWriter) {
        String path = file.getPath();
        String dependentFilePath = "";
        String fileListPath = fileList.getParentFile().getPath();
        if (path.startsWith(fileListPath)) {
            path = path.substring(fileListPath.length() + 1);
        } else {
            log.warn("Dump file {} is located not above {}. Skip storing it", file, fileListPath);
            return;
        }
        if(file.getDependentFile() != null) {
            dependentFilePath = file.getDependentFile().getPath().substring(fileListPath.length() + 1);
        }

        if (printWriter == null) {
            log.warn("Can't write in log line \"{},{},{},{}\"", operation, path, file.getTimestamp(), file.getSize());
            return;
        }
        try {
            printWriter.write(operation);
            printWriter.write(path);
            printWriter.writeVarInt(file.getTimestamp());
            printWriter.writeVarInt(file.getSize());
            printWriter.write(dependentFilePath);
            printWriter.flush();
            logEntryCount++;
            justCreated = false;
        } catch (IOException e) {
            log.error("Error during writing to log file {}", fileList, e);
        }
    }

    public synchronized void close() {
        IOHelper.close(outputStream);
        outputStream = null;
    }
}
