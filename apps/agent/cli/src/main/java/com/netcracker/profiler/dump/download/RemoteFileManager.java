package com.netcracker.profiler.dump.download;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RemoteFileManager implements Closeable {
    private static final Pattern LS_REG_EXP = Pattern.compile("\\s*([\\w-])\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\d+)\\s+(\\d+)\\s+(.+)\\s*");
    private JSch jsch;
    private String remoteHost;
    private String userName;
    private String password;
    private Session session;
    private ProfilerChannelShell channelShell;
    private String shell;
    private Map<String, RemoteFile> filesCache = new HashMap<>();


    public RemoteFileManager(String remoteHost, String userName, String password, String shell) throws IOException {
        try {
            this.jsch = JSchConfig.createJSch();
        } catch (JSchException e) {
            throw new IOException(e);
        }
        this.remoteHost = remoteHost;
        this.userName = userName;
        this.password = password;
        this.shell = shell;
    }

    public void connect() throws IOException {
        try {
            createSession(remoteHost, userName, password);
            createChannelShell();
        } catch (JSchException e) {
            throw new IOException(e);
        }
    }

    public synchronized RemoteFile getFile(String path) {
        return getFile(path, false);
    }

    public synchronized RemoteFile getFile(String path, boolean fromCache) {
        RemoteFile file = filesCache.get(path);
        if (fromCache) {
            return file;
        }
        if (file == null) {
            file = new RemoteFile(path, this);
            filesCache.put(path, file);
        }
        return file;
    }

    public synchronized void preloadRecursiveChild(RemoteFile file) throws IOException {
        preloadRecursiveChild(file, null);
    }

    public synchronized void preloadRecursiveChild(RemoteFile file, String ignore) throws IOException {
        String command = "ls -l -L -R --time-style +%s --color=none " + (ignore == null ? "" : " -I " + ignore) + " " + file.getShellQuotedPath();
        String recursiveFilesList = execCommand(command);
        parseLsROutput(recursiveFilesList, file);
    }

    synchronized List<RemoteFile> listFiles(RemoteFile file) throws IOException {
        String lsOutput = execCommand("ls -l -L --time-style +%s --color=none " + file.getShellQuotedPath());
        parseLsOutput(lsOutput, file);
        return file.childFiles;
    }

    synchronized void readAttrs(final RemoteFile file) throws IOException {
        try {
            String lsOutput = execCommand("ls -ld -L --time-style +%s --color=none " + file.getShellQuotedPath());
            parseLsLine(lsOutput, file);
        } catch (ExecCommandException e) {
            if (e.getMessage() != null && e.getMessage().contains("No such file or directory")) {
                file.fileExists = false;
            } else {
                throw e;
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        filesCache.clear();
        if (channelShell != null) {
            channelShell.close();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    public synchronized String execCommand(String command) throws IOException {
        return channelShell.execCommand(command);
    }

    private void parseLsOutput(String lsOutput, RemoteFile root) {
        root.childFiles = new ArrayList<>();
        Scanner lsScanner = new Scanner(lsOutput);
        lsScanner.nextLine(); // skip total
        while (lsScanner.hasNextLine()) {
            RemoteFile file = parseLsLine(lsScanner.nextLine(), root.getPath());
            root.childFiles.add(file);
            filesCache.put(file.getPath(), file);
        }
    }

    private void parseLsROutput(String lsROutput, RemoteFile root) {
        Scanner lsRScanner = new Scanner(lsROutput);
        String curDir;
        String line;
        while (lsRScanner.hasNextLine()) {
            line = lsRScanner.nextLine();
            curDir = line.substring(0, line.length() - 1);
            lsRScanner.nextLine(); // skip total
            RemoteFile curDirFile = getFile(curDir);
            curDirFile.childFiles = new ArrayList<>();

            while (lsRScanner.hasNextLine()) {
                line = lsRScanner.nextLine();
                if (line.isEmpty()) {
                    break;
                }
                RemoteFile file = parseLsLine(line, curDir);
                curDirFile.childFiles.add(file);
                filesCache.put(file.getPath(), file);
            }

        }
    }

    private RemoteFile parseLsLine(String lsLine, RemoteFile file) {
        return parseLsLine(lsLine, null, file);
    }

    private RemoteFile parseLsLine(String lsLine, String path) {
        return parseLsLine(lsLine, path, null);
    }

    private RemoteFile parseLsLine(String lsLine, String path, RemoteFile file) {
        Matcher matcher = LS_REG_EXP.matcher(lsLine);
        if (!matcher.matches()) {
            throw new RuntimeException("LS_REG_EXP doesn't match line: " + lsLine);
        }
        String type = matcher.group(1);
        String length = matcher.group(2);
        String modificationTimestamp = matcher.group(3);
        String fileName = matcher.group(4);

        if (file == null) {
            file = new RemoteFile(path + RemoteFile.separatorChar + fileName, this);
        }
        file.isDirectory = type.equals("d");
        file.isFile = type.equals("-");
        file.lastModificationTime = Long.parseLong(modificationTimestamp) * 1000l;
        file.length = Long.parseLong(length);
        file.fileExists = true;

        return file;
    }

    private void createSession(String remoteHost, String userName, String password) throws JSchException {
        session = jsch.getSession(userName, remoteHost);
        JSchConfig.applyToSession(session);
        session.setPassword(password);
        session.connect();
    }

    private void createChannelShell() throws JSchException, IOException {
        channelShell = new ProfilerChannelShell(session, shell);
        channelShell.connect();
    }
}
