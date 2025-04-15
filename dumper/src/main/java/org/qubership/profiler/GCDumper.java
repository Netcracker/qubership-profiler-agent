package org.qubership.profiler;

import org.qubership.profiler.agent.PropertyFacadeBoot;
import org.qubership.profiler.stream.ICompressedLocalAndRemoteOutputStream;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GCDumper implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(GCDumper.class);

    private static final String SHOULD_HARVEST_GC_LOG = PropertyFacadeBoot.getPropertyOrEnvVariable("ESC_HARVEST_GCLOG");

    private static final Pattern[] GC_LOG_FILE_PATTERNS = new Pattern[]{
            Pattern.compile("-Xloggc:(.+)"),
            Pattern.compile("-Xlog.*:file=(..[^:]+)")
    };

    private static final Pattern[] NUM_GC_LOGS_PATTERNS = new Pattern[]{
            Pattern.compile("-XX:NumberOfGCLogFiles=([0-9]+)"),
            Pattern.compile("-Xlog.*:filecount=([0-9]+)")
    };

    private WatchService watcher;

    private ICompressedLocalAndRemoteOutputStream out;
    private FileInputStream gcInput;
    File gcFolderPath;
    File gcLogFile;
    long alreadyRead;
    int numGCLogFiles;
    String gcLogFileName;
    private boolean enabled = false;
    private boolean absentGCLogReported=false;

    private boolean isJava11 = false;

    private File getGcLogFile(){
        for(String arg: ManagementFactory.getRuntimeMXBean().getInputArguments()){
            for(Pattern p: GC_LOG_FILE_PATTERNS){
                Matcher m = p.matcher(arg);
                if(m.find()){
                    File theFile = new File(m.group(1)).getAbsoluteFile();
                    File theFolder = theFile.getParentFile();
                    if(theFolder.exists() && theFolder.isDirectory()){
                        logger.debug("Detected gc log file for export {}", theFile.getAbsolutePath());
                        return theFile;
                    }
                }
            }
        }
        logger.debug("No GC logs detected");
        return null;
    }

    private int getNumGCLogFiles(){
        for(String arg: ManagementFactory.getRuntimeMXBean().getInputArguments()){
            for(Pattern p: NUM_GC_LOGS_PATTERNS){
                Matcher m = p.matcher(arg);
                if(m.find()){
                    String theNumber = m.group(1);
                    if(!StringUtils.isBlank(theNumber) && StringUtils.isNumeric(theNumber)){
                        logger.debug("detected number of GC log files {}", theNumber);
                        return Integer.parseInt(theNumber);
                    }
                }
            }
        }
        logger.debug("Can not detect number of GC log files. Asuming that GC is not rotated");
        return 0;
    }

    private void detectJavaVersion () {
        String javaVersion = System.getProperty("java.specification.version");
        if(StringUtils.isNumeric(javaVersion)){
            isJava11 = Integer.parseInt(javaVersion) >= 11;
        }
        logger.trace("Is java 11: {}", isJava11);
    }

    public GCDumper(ICompressedLocalAndRemoteOutputStream out) {
        detectJavaVersion();
        if(!"true".equalsIgnoreCase(SHOULD_HARVEST_GC_LOG)){
            return;
        }

        this.enabled = true;

        File gcLogFileBase = getGcLogFile();
        if(gcLogFileBase == null) {
            return;
        }
        gcFolderPath = gcLogFileBase.getParentFile();
        gcLogFileName = gcLogFileBase.getName();
        this.numGCLogFiles = getNumGCLogFiles();
        this.out = out;
        List<File> potentialCurrentLogs = new ArrayList<>();
        for(File f: gcFolderPath.listFiles()){
            if(f.getName().startsWith(gcLogFileName) && (
                            f.getName().endsWith(".current") || //for java 8
                            f.getName().equals(gcLogFileName) // for java 11
            )){
                potentialCurrentLogs.add(f);
            }
        }
        Collections.sort(potentialCurrentLogs, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long delta = -(o1.lastModified() - o2.lastModified());
                return delta > 0? 1 : delta == 0? 0: -1;
            }
        });
        if(potentialCurrentLogs.size() > 0){
            gcLogFile = potentialCurrentLogs.get(0);
        }

        try {
            watcher = FileSystems.getDefault().newWatchService();
            Path gcLogPath = gcFolderPath.toPath();
            gcLogPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void reopenLog() throws IOException {
        try {
            closeGcInput();
        } catch (IOException e){
            logger.warn("Failed to close previous gc log file ", e);
        }

        if(gcLogFile == null) {
            logger.info("Log file has not been detected in {} yet", gcFolderPath);
            return;
        }

        logger.trace("reopenning gc log file {}", gcLogFile.getAbsolutePath());
        try{
            this.gcInput = new FileInputStream(gcLogFile);
        } catch (FileNotFoundException e){
            if(gcLogFile.getName().endsWith(".current")){
                logger.trace("File {} not found. trying without .current extension", this.gcLogFile.getAbsolutePath());
                this.gcLogFile = new File(gcFolderPath, nameNoCurrent(this.gcLogFile));
                this.gcInput = new FileInputStream(gcLogFile);
            }
        }
        this.alreadyRead = 0;
    }

    private List<File> collectModifiedFiles(){
        List<File> modifiedFiles = new ArrayList<>();
        WatchKey wk;
        while((wk=watcher.poll()) != null) {
            for (WatchEvent we : wk.pollEvents()) {
                Path p = (Path) we.context();
                File newGCLog = p.toFile();
                if(logger.isTraceEnabled()) {
                    logger.trace("received event for file {}:{}", we.kind(), newGCLog.getAbsolutePath());
                }

                if(!newGCLog.getName().startsWith(gcLogFileName)){
                    if(logger.isTraceEnabled()){
                        logger.trace("skipping modify event for {} since it does not match base gc log file name {}", newGCLog.getName(), gcLogFileName);
                    }
                    continue;
                }

                if(newGCLog.equals(gcLogFile)){
                    continue;
                }
                modifiedFiles.add(newGCLog);
            }
            wk.reset();
        }
        return modifiedFiles;
    }

    private TreeMap<Integer, String> mapGCLogIndexes(List<File> modifiedFiles){
        TreeMap<Integer, String> indexesToNames = new TreeMap<>();
        if(modifiedFiles.size() > 0) {
            Set<String> alreadyPresent = new HashSet<>();
            for (File f : modifiedFiles) {
                String name = nameNoCurrent(f);
                if (alreadyPresent.contains(name)) {
                    continue;
                }
                f = tryFindGCLog(name);
                if (f == null) {
                    logger.warn("failed to find gc log {}. file does not exist", name);
                    continue;
                }
                indexesToNames.put(indexFromName(name), name);
                alreadyPresent.add(name);
            }
        }
        return indexesToNames;
    }

    private List<String> retainOnlyThoseFollowingCurrentGCFile(TreeMap<Integer, String> indexesToNames){
        List<String> namesToDump = new ArrayList<>();
        if(gcLogFile == null){
            namesToDump.addAll(indexesToNames.values());
        } else {
            int activeIndex = indexFromName(nameNoCurrent(gcLogFile));
            for(int i=1; i < numGCLogFiles; i++){
                int indexOfLog = (i+activeIndex) % numGCLogFiles;
                if(indexesToNames.containsKey(indexOfLog)){
                    namesToDump.add(indexesToNames.get(indexOfLog));
                } else {
                    break;
                }
            }
        }
        return namesToDump;
    }

    private boolean shouldReopenLog() throws IOException {
        boolean result = false;

        //JDK 8 will write to <log>.<index>.current and cycle through indexes
        //JDK 11 will write only to <log> and rename it to <log>.<index> when it's full
        if(!isJava11) {
            List<File> modifiedFiles = collectModifiedFiles();

            TreeMap<Integer, String> indexesToNames = mapGCLogIndexes(modifiedFiles);

            List<String> namesToDump = retainOnlyThoseFollowingCurrentGCFile(indexesToNames);

            if (namesToDump.size() > 0) {
                String lastFile = namesToDump.remove(namesToDump.size() - 1);
                for (String nameToDump : namesToDump) {
                    logger.trace("intermediate GC log dumped {}", nameToDump);
                    dumpFile(nameToDump);
                }

                result = true;
                gcLogFile = tryFindGCLog(lastFile);
                if (gcLogFile == null) {
                    logger.trace("Failed to find an active GC log file by file name {}", lastFile);
                    return false;
                } else {
                    logger.trace("New gc log file to follow {}", gcLogFile.getAbsolutePath());
                }
            }
        }

        return result || gcInput == null || gcLogFile.length() < alreadyRead ;
    }

    int indexFromName(String name) {
        int dotIndex = name.lastIndexOf('.');
        return Integer.parseInt(name.substring(dotIndex+1));
    }

    String nameNoCurrent(File f){
        String name = f.getName();
        if(name.endsWith(".current")){
            name = name.substring(0, name.length()-".current".length());
        }
        return name;
    }

    private void finishPreviousDump() throws IOException {
        if(gcInput != null) {
            readTillTheEnd();
            out.getStream().flush();
            out.rotate();
        }
    }

    private void dumpFile(String name) throws IOException {
        finishPreviousDump();

        gcLogFile = tryFindGCLog(name);
        if(gcLogFile == null) {
            logger.warn("Failed to find gc log file by name {}", name);
            return;
        }
        logger.trace("New log file to dump {}", gcLogFile.getAbsolutePath());
        reopenLog();
        readTillTheEnd();
        closeGcInput();
        gcLogFile = null;
    }

    private File tryFindGCLog(String name){
        File f = new File(gcFolderPath, name);
        if(f.exists()) {
            return f;
        }
        f = new File(gcFolderPath, name + ".current");
        if(f.exists()){
            return f;
        }
        return null;
    }

    public void dumpGC() throws IOException{
        //do not export GC if it has not been explicitly requested
        if(!enabled){
            return;
        }
        if(gcLogFile == null) {
            if(!absentGCLogReported) {
                logger.warn("gc log is absent. not dumping GC");
                absentGCLogReported=true;
            }
            return;
        }
        if(!gcLogFile.exists()){
            logger.warn("File {} does not exist. can not export GC log", gcLogFile.getAbsolutePath());
            return;
        }
        if(gcInput != null) {
            readTillTheEnd();
        }
        if(shouldReopenLog()){
            if(logger.isTraceEnabled()) {
                logger.trace("Reopenning GC log {}", gcLogFile.getAbsolutePath());
            }
            if(gcInput != null) {
                logger.trace("Rotating the stream");
                out.getStream().flush();
                out.rotate();
            }
            reopenLog();
        }
        if(gcInput != null) {
            readTillTheEnd();
        }
    }

    private void readTillTheEnd() throws IOException {
        byte[] buf = new byte[1024];
        while(true){
            int len;
            try {
                len = gcInput.read(buf);
            } catch (IOException e) {
                logger.error("Failed to read from gc log {}", gcFolderPath.getAbsolutePath(), e);
                try {
                    closeGcInput();
                } finally {
                    gcInput = null;
                }
                //do not fail the whole dumper when reading from GC log fails
                break;
            }
            if(len > 0) {
                logger.trace("Sending {} bytes to gc log", len);
                alreadyRead += len;
                out.getStream().write(buf, 0, len);
            } else {
                logger.trace("Read {} bytes from GC log", len);
                break;
            }
        }
    }

    private void closeGcInput() throws IOException {
        if (gcInput != null) {
            gcInput.close();
        }
    }

    @Override
    public void close() throws IOException {
        logger.trace("closing GC dumper");
        try {
            closeGcInput();
        } finally {
            if(watcher != null) {
                watcher.close();
            }
        }

    }
}
