package com.netcracker.profiler.dump.download;

import com.netcracker.profiler.dump.download.RemoteFile.FileFilter;
import com.netcracker.profiler.io.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DumpDownloader implements Closeable {
    public static final Logger log = LoggerFactory.getLogger(DumpDownloader.class);
    final static NumberFormat fileIndexFormat = NumberFormat.getIntegerInstance();

    static {
        fileIndexFormat.setGroupingUsed(false);
        fileIndexFormat.setMinimumIntegerDigits(6);
    }

    protected static final FileFilter DIRECTORY_FILTER = pathname -> {
        try {
            return pathname.isDirectory();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    protected final static FileFilter YEAR_DIRECTORY_FILTER = pathname -> {
        try {
            return pathname.isDirectory() && pathname.getName().length() == 4 && containsOnlyDigits(pathname.getName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    protected final static FileFilter NUMBER_DIRECTORY_FILTER = pathname -> {
        try {
            return pathname.isDirectory() && containsOnlyDigits(pathname.getName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    protected final static Comparator<RemoteFile> MODIFICATION_DATE_FILE_COMPARATOR = new Comparator<RemoteFile>() {
        @Override
        public int compare(RemoteFile o1, RemoteFile o2) {
            try {
                return Long.compare(o1.lastModified(), o2.lastModified());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    /*
        (PROCESS_LIST=`ps auxww | grep -v grep | grep -E 'javaagent:\S*execution-statistics-collector\S*agent.jar' | awk '{print $2}'`;
        for PROCESS in $PROCESS_LIST;
        do
            PWDX=`pwdx $PROCESS 2> /dev/null | awk '{print $2}'`;
            if [ "${PWDX}" = "" ]; then
                PWDX=`ps $PROCESS | grep -E 'Dnetcracker.home=' | sed  's/^.*Dnetcracker\.home=\(\S\+\).*$/\1/g' | xargs -r realpath 2> /dev/null`;
            fi;

            PDUMP=`ps $PROCESS | grep -E 'Dprofiler.dump=' | sed  's/^.*Dprofiler\.dump=\(\S\+\).*$/\1/g' | xargs -r dirname`;
            if [ "${PDUMP}" == "" ]; then
                PDUMP=`ps $PROCESS | grep -E 'Dprofiler.dump.home=' | sed  's/^.*Dprofiler\.dump\.home=\(\S\+\).*$/\1/g'`;
            fi;
            if [ "${PDUMP}" != "" ]; then
                if [[ $PDUMP == /* ]]; then
                    if test -d $PDUMP; then echo $PDUMP; fi;
                else
                    if test -d $PWDX'/'$PDUMP; then echo $PWDX'/'$PDUMP; fi;
                fi;
                continue;
            fi;

            AGENT=`ps $PROCESS | grep -E 'javaagent:\S*execution-statistics-collector\S*agent.jar' | sed  's/^.*javaagent:\(\S*execution-statistics-collector\S\+\).*$/\1/g'`;
            if [[ $AGENT == /* ]]; then
                DUMP=`realpath $AGENT 2> /dev/null | xargs -r dirname | xargs -r dirname | xargs -r dirname | xargs -r dirname`'/execution-statistics-collector/dump';
                if test -d $DUMP; then echo $DUMP; fi;
            else
                DUMP=$PWDX'/execution-statistics-collector/dump';
                if test -d $DUMP; then echo $DUMP; fi;
            fi;
        done;) | uniq | xargs -r realpath 2> /dev/null;
    */
    protected final static String FIND_DUMP_ROOT_SCRIPT =
            "(PROCESS_LIST=`ps auxww | grep -v grep | grep -E 'javaagent:\\S*execution-statistics-collector\\S*agent.jar' | " +
                    "awk '{print $2}'`; for PROCESS in $PROCESS_LIST; do PWDX=`pwdx $PROCESS 2> /dev/null | awk '{print $2}'`; " +
                    "if [ \"${PWDX}\" = \"\" ]; then " +
                    " PWDX=`ps $PROCESS | grep -E 'Dnetcracker.home=' | sed 's/^.*Dnetcracker\\.home=\\(\\S\\+\\).*$/\\1/g' | " +
                    "xargs -r realpath 2> /dev/null`; fi;  PDUMP=`ps $PROCESS | grep -E 'Dprofiler.dump=' | " +
                    "sed 's/^.*Dprofiler\\.dump=\\(\\S\\+\\).*$/\\1/g' | xargs -r dirname`; if [ \"${PDUMP}\" == \"\" ]; " +
                    "then  PDUMP=`ps $PROCESS | grep -E 'Dprofiler.dump.home=' | " +
                    "sed 's/^.*Dprofiler\\.dump\\.home=\\(\\S\\+\\).*$/\\1/g'`; fi; " +
                    "if [ \"${PDUMP}\" != \"\" ]; then  if [[ $PDUMP == /* ]]; then  if test -d $PDUMP; then echo $PDUMP; fi;  " +
                    "else  " +
                    "if test -d $PWDX'/'$PDUMP; then echo $PWDX'/'$PDUMP; fi;  " +
                    "fi;  continue; fi;  AGENT=`ps $PROCESS | " +
                    "grep -E 'javaagent:\\S*execution-statistics-collector\\S*agent.jar' | " +
                    "sed 's/^.*javaagent:\\(\\S*execution-statistics-collector\\S\\+\\).*$/\\1/g'`; " +
                    "if [[ $AGENT == /* ]]; then  DUMP=`realpath $AGENT 2> /dev/null | " +
                    "xargs -r dirname | xargs -r dirname | xargs -r dirname | xargs -r dirname`'/execution-statistics-collector/dump';  " +
                    "if test -d $DUMP; then echo $DUMP; fi; else  DUMP=$PWDX'/execution-statistics-collector/dump';  " +
                    "if test -d $DUMP; then echo $DUMP; fi; fi; done;) | uniq | xargs -r realpath 2> /dev/null;";
    protected final static String GET_SERVER_TIMEZONE_COMMAND = "date +%:z";

    private static boolean containsOnlyDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private RemoteFileManager remoteFileManager;
    private Downloader downloader;
    private long startDate;
    private long endDate;

    private String endPath;
    private boolean skipDetails;

    private String outputFolder;

    private List<String> selectedServers;
    private String currentServer;
    private RemoteFile dumpRoot;
    private List<Pair<RemoteFile, File>> filesToDownload = new ArrayList<>();

    public void connect(String host, String user, String password) throws IOException {
        connect(host, user, password, 1);
    }

    public void connect(String host, String user, String password, int parallelDegree) throws IOException {
        connect(host, user, password, parallelDegree, null);
    }

    public void connect(String host, String user, String password, int parallelDegree, String localUser) throws IOException {
        connect(host, user, password, parallelDegree, localUser, DownloadProtocol.SFTP);
    }

    public void connect(String host, String user, String password, int parallelDegree, String shell, DownloadProtocol downloadProtocol) throws IOException {
        this.remoteFileManager = new RemoteFileManager(host, user, password, shell);
        remoteFileManager.connect();
        if (downloadProtocol == DownloadProtocol.SCP) {
            this.downloader = new SCPDownloader(host, user, password, shell, parallelDegree);
        } else {
            if (shell != null) {
                throw new IllegalArgumentException("SFTP protocol doesn't support using of local user");
            }
            this.downloader = new SFTPDownloader(host, user, password, parallelDegree);
        }
        this.downloader.connect();
    }

    public TimeZone getServerTimezone() throws IOException {
        String timeZoneStr = "GMT" + remoteFileManager.execCommand(GET_SERVER_TIMEZONE_COMMAND).trim();
        return TimeZone.getTimeZone(timeZoneStr);
    }

    public List<String> findDumpRoots() throws IOException {
        String result = remoteFileManager.execCommand(FIND_DUMP_ROOT_SCRIPT);
        if (result == null || result.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> dumpRoots = new ArrayList<>();
        Scanner scanner = new Scanner(result);
        while (scanner.hasNextLine()) {
            dumpRoots.add(scanner.nextLine());
        }
        return dumpRoots;
    }

    public List<Pair<RemoteFile, File>> collectFilesToDownload(long startDate, long endDate, boolean skipDetails, List<String> selectedServers,
                                                               String dumpRootPath, String outputFolder) throws IOException {
        this.startDate = startDate;
        this.endDate = endDate;
        this.skipDetails = skipDetails;
        this.selectedServers = selectedServers;
        this.dumpRoot = getDumpRoot(dumpRootPath);
        this.outputFolder = outputFolder;
        collectFilesToDownloadInner();
        return filesToDownload;
    }

    public DownloadProgress startDownloading() throws IOException {
        return downloader.downloadAsync(filesToDownload);
    }

    private void collectFilesToDownloadInner() throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("'" + RemoteFile.separatorChar + "'yyyy'" + RemoteFile.separatorChar + "'MM'" + RemoteFile.separatorChar + "'dd");
        endPath = endDate == Long.MAX_VALUE ? null : sdf.format(endDate) + RemoteFile.separatorChar + endDate;

        List<RemoteFile> servers = dumpRoot.listFiles(DIRECTORY_FILTER);
        if (servers == null || servers.isEmpty()) {
            log.warn("No data found in dump root {}", dumpRoot.getPath());
            return;
        }

        for (RemoteFile server : servers) {
            currentServer = server.getName();
            if (selectedServers != null && !selectedServers.contains(currentServer)) {
                continue;
            }
            findInFolder(server, "", 0, Long.MAX_VALUE);
        }
    }

    private void findInFolder(RemoteFile root, String currentPath, int level, long dateUpperBound) throws IOException {
        if (level != 0 && endPath != null && currentPath.compareTo(endPath) > 0) {
            return;
        }
        if (level == 4) {
            if (dateUpperBound < startDate) {
                return;
            }

            boolean addedCalls = false;
            for (String folderName : getCallsFolders(root)) {
                addedCalls = appendChildFilesByDate(currentPath, root, folderName) || addedCalls;
            }
            if (!addedCalls) {
                return;
            }

            appendAllChildFiles(currentPath, root, "dictionary");
            appendAllChildFiles(currentPath, root, "callsDictionary");
            appendAllChildFiles(currentPath, root, "params");
            appendAllChildFiles(currentPath, root, "suspend");
            if (!skipDetails) {
                appendChildFilesByDate(currentPath, root, "trace");
                appendChildFilesByDate(currentPath, root, "xml");
                appendAllChildFiles(currentPath, root, "sql");
            }
            return;
        }

        if (root.isDirectory()) {
            final List<RemoteFile> files = root.listFiles(level == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
            if (files == null || files.isEmpty()) {
                return;
            }
            Collections.sort(files);
            for (int i = 0; i < files.size(); i++) {
                RemoteFile f = files.get(i);
                final String fileName = f.getName();
                final String nextPath = currentPath + RemoteFile.separatorChar + fileName;
                long upperBound;
                if (level == 3 && i + 1 < files.size()) {
                    upperBound = Long.parseLong(files.get(i + 1).getName());
                } else {
                    RemoteFile firstDir = findFirstDir(f, level + 1);
                    upperBound = firstDir == null ? Long.MAX_VALUE : Long.parseLong(firstDir.getName());
                }
                findInFolder(f, nextPath, level + 1, upperBound);
            }
        }
    }

    private List<String> getCallsFolders(RemoteFile root) throws IOException {
        List<String> callsFolders = new ArrayList<>();
        for (RemoteFile folder : root.listFiles(DIRECTORY_FILTER)) {
            String folderName = folder.getName();
            if ("calls".equals(folderName) || folderName.startsWith("calls[")) {
                callsFolders.add(folderName);
            }
        }
        return callsFolders;
    }

    private RemoteFile findFirstDir(RemoteFile root, int level) throws IOException {
        if (level == 0) {
            return null;
        }
        List<RemoteFile> files = root.getParentFile().listFiles(level - 1 == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
        if (files == null || files.isEmpty()) {
            return null;
        }
        String rootName = root.getName();
        RemoteFile next = null;
        String nextName = null;
        for (RemoteFile file : files) {
            String name = file.getName();
            if (name.compareTo(rootName) <= 0) {
                continue;
            }
            if (nextName == null || nextName.compareTo(name) > 0) {
                next = file;
                nextName = name;
            }
        }

        if (next == null) {
            return findFirstDir(root.getParentFile(), level - 1);
        }

        return findFirstLogDir(next, level);
    }

    private RemoteFile findFirstLogDir(RemoteFile root, int level) throws IOException {
        if (root.isDirectory()) {
            final List<RemoteFile> files = root.listFiles(level == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
            if (files == null || files.isEmpty()) {
                return null;
            }
            if (level == 3) {
                RemoteFile min = Collections.min(files);
                return min;
            }
            Collections.sort(files);
            for (RemoteFile f : files) {
                RemoteFile res = findFirstLogDir(f, level + 1);
                if (res != null) {
                    return res;
                }
            }
        }
        return null;
    }

    private boolean appendChildFilesByDate(String relativePath, RemoteFile root, String folderName) throws IOException {
        boolean added = false;
        String targetDir = getTargetPath(relativePath, folderName);
        RemoteFile folder = root.childFile(folderName, true);
        if (folder == null || !folder.exists()) {
            return false;
        }

        List<RemoteFile> childFiles = folder.listFiles();
        Collections.sort(childFiles, MODIFICATION_DATE_FILE_COMPARATOR);
        int i = 0;
        for (; i < childFiles.size(); i++) {
            if (childFiles.get(i).lastModified() >= startDate) {
                break;
            }
        }

        for (; i < childFiles.size(); i++) {
            added = true;
            RemoteFile file = childFiles.get(i);
            if (file.lastModified() > endDate) {
                appendFile(file, targetDir); //Download one more file after reaching lastModified() > endDate
                break;
            }
            appendFile(file, targetDir);
        }
        return added;
    }

    private void appendAllChildFiles(String relativePath, RemoteFile root, String folderName) throws IOException {
        String targetDir = getTargetPath(relativePath, folderName);
        RemoteFile folder = root.childFile(folderName, true);
        if (folder == null || !folder.exists()) {
            return;
        }

        List<RemoteFile> childFiles = folder.listFiles();
        for (RemoteFile file : childFiles) {
            appendFile(file, targetDir);
        }
    }

    private void appendFile(RemoteFile remoteFile, String targetDir) throws IOException {
        File localFile = new File(targetDir, remoteFile.getName());
        if (localFile.exists() && localFile.length() == remoteFile.length()) {
            return;
        }
        filesToDownload.add(new Pair<>(remoteFile, localFile));
    }

    private RemoteFile getDumpRoot(String dumpRootPath) throws IOException {
        RemoteFile dumpRootFile = remoteFileManager.getFile(dumpRootPath);
        if (!dumpRootFile.exists()) {
            throw new FileNotFoundException("Dump root folder '" + dumpRootPath + "' doesn't exist");
        }
        remoteFileManager.preloadRecursiveChild(dumpRootFile, "classes");
        return dumpRootFile;
    }

    private String getTargetPath(String relativePath, String folderName) {
        String targetDir = outputFolder + File.separatorChar + currentServer + File.separatorChar + relativePath + File.separatorChar + folderName;
        return new File(targetDir).getAbsolutePath();
    }

    @Override
    public void close() throws IOException {
        if (remoteFileManager != null) {
            remoteFileManager.close();
        }
        if (downloader != null) {
            downloader.close();
        }
    }
}
