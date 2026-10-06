package com.netcracker.profiler.dump.download;

import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ProfilerChannelShell implements Closeable {
    private static final String SHELL_LOG_FOLDER = System.getProperty("ProfilerChannelShell.SHELL_LOG_FOLDER");
    private static final int SHELL_INIT_TIMEOUT_SEC = Integer.getInteger("ProfilerChannelShell.SHELL_INIT_TIMEOUT_SEC", 60);
    // Unique shell prompt to detect command completion reliably
    // Must be a string that won't appear in normal command output
    private static final String SHELL_PROMPT = "swdbikNZwLk1rV7X5u8WegWQDbY0GQAyENvJ1yZXGUi0XdNvaQrp0S8NPX9ruUda]";
    // Unique error start to detect command completion reliably
    // Must be a string that won't appear in normal command output
    private static final String ERROR_START = "ewdbikNZwLk1rV7X5u8WegWQDbY0GQAyENvJ1yZXGUi0XdNvaQrp0S8NPX9ruUda]";
    // Unique sync label to detect command completion reliably
    // Must be a string that won't appear in normal command output
    private static final String SYNC_LABEL = "syncikNZwLk1rV7X5u8WegWQDbY0GQAyENvJ1yZXGUi0XdNvaQrp0S8NPX9ruUda";
    private static final AtomicInteger shellLogNumber = new AtomicInteger();
    private Session session;
    private boolean raw;
    private String shell;
    private ChannelShell channelShell;
    private PrintWriter shellWritter;
    private BufferedReader shellReader;
    private int syncIter;
    private PrintWriter shellLogWriter;

    final Thread SHUTDOWN_HOOK = new Thread() {
        @Override
        public void run() {
            if (shellLogWriter != null) {
                shellLogWriter.close(); //To flush logs if aborted
            }
        }
    };

    public ProfilerChannelShell(Session session, String shell) {
        this(session, shell, false);
    }

    public ProfilerChannelShell(Session session, String shell, boolean raw) {
        this.session = session;
        this.raw = raw;
        this.shell = shell;
        Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }

    public synchronized void connect() throws IOException {
        try {
            channelShell = (ChannelShell) session.openChannel("shell");
            channelShell.connect();
            shellWritter = new PrintWriter(channelShell.getOutputStream(), true);
            shellReader = new BufferedReader(new InputStreamReader(channelShell.getInputStream()));
            if (SHELL_LOG_FOLDER != null) {
                shellLogWriter = new PrintWriter(new BufferedOutputStream(new FileOutputStream(new File(SHELL_LOG_FOLDER, "profilerShellLog" + shellLogNumber.incrementAndGet() + ".log"))));
            }
            execCommandNoWait(""); //Skip intro
            syncOrFail();
            execCommandNoWait("stty -echo");
            execCommandNoWait("bash");
            execCommandNoWait(""); //Skip intro
            syncOrFail();
            if (shell != null) {
                execCommandNoWait(shell);
                execCommandNoWait(""); //Skip intro
                syncOrFail();
            }
            execCommand("export PS1='" + SHELL_PROMPT + "'", false);
            if (raw) {
                execCommand("stty raw", false);
            }
        } catch (JSchException e) {
            throw new RuntimeException(e);
        }

    }

    public synchronized String execCommand(String command) throws IOException {
        return execCommand(command, true);
    }

    public synchronized String execCommand(String command, boolean checkErrors) throws IOException {
        if (checkErrors) {
            command = "{(" + command + ") 2>&1 1>&3 | (sed 's/^/" + ERROR_START + "/');} 3>&1";
        }
        execCommandNoWait(command);
        String result = readTillPrompt();
        if (checkErrors) {
            checkForErrors(result);
        }
        return result;
    }

    synchronized void execCommandNoWait(String command) throws IOException {
        command += '\n';
        if (shellLogWriter != null) {
            shellLogWriter.print(">>ExecCommand: " + command + ">>");
        }
        shellWritter.print(command);
        shellWritter.flush();
    }

    public synchronized void sendSignal(String signal) throws IOException {
        try {
            channelShell.sendSignal(signal);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    String readTillString(String string) throws IOException {
        StringBuilder outputBuffer = new StringBuilder();
        readTillString(string, outputBuffer);
        return outputBuffer.toString();
    }

    void readTillString(String string, StringBuilder outputBuffer) throws IOException {
        StringBuilder stringBuffer = new StringBuilder(string.length());
        int i = 0;
        while (true) {
            int b = shellReader.read();
            if (b == -1) {
                throw new IOException("Unexpected end of shell inputStream");
            }
            char c = (char) b;
            if (shellLogWriter != null) {
                shellLogWriter.write(c);
            }
            if (c == string.charAt(i)) {
                stringBuffer.append(c);
                i++;
                if (i == string.length()) {
                    return;
                }
            } else {
                outputBuffer.append(stringBuffer);
                outputBuffer.append(c);
                stringBuffer.setLength(0);
                i = 0;
            }
        }
    }

    boolean readTillString(String string, StringBuilder outputBuffer, int timeout) throws IOException {
        DownloadTimeoutHandler.scheduleTimeout(timeout);
        try {
            readTillString(string, outputBuffer);
            return true;
        } catch (InterruptedIOException e) {
            return false;
        } finally {
            DownloadTimeoutHandler.cancelTimeout();
        }
    }

    String readTillPrompt() throws IOException {
        return readTillString(SHELL_PROMPT);
    }

    void readTillPrompt(StringBuilder outputBuffer) throws IOException {
        readTillString(SHELL_PROMPT, outputBuffer);
    }

    boolean readTillPrompt(StringBuilder outputBuffer, int timeout) throws IOException {
        return readTillString(SHELL_PROMPT, outputBuffer, timeout);
    }

    private void checkForErrors(String s) throws ExecCommandException {
        int i = 0;
        boolean inErrorBlock = false;
        for (int j = 0; j < s.length(); j++) {
            char c = s.charAt(j);
            if (inErrorBlock) {
                if (c == '\n') {
                    throw new ExecCommandException(s.substring(j - i, j - 1));
                }
                i++;
            } else if (c == ERROR_START.charAt(i)) {
                i++;
                if (i == ERROR_START.length()) {
                    inErrorBlock = true;
                    i = 0;
                }
            } else {
                i = 0;
            }
        }
    }

    private boolean sync() throws IOException {
        boolean done = false;
        StringBuilder outputBuffer = new StringBuilder();
        int totalWaitTime = 0;
        int timeout = 100;
        while (true) {
            syncIter++;
            String syncLabel = SYNC_LABEL + syncIter + "]";
            execCommandNoWait("echo " + syncLabel + ";");
            done = readTillString(syncLabel + "\r\n", outputBuffer, timeout);
            if (done) {
                return true;
            }
            if (outputBuffer.length() > 0 && outputBuffer.toString().contains(syncLabel + "\r\n")) {
                return true;
            }
            totalWaitTime += timeout;
            if (totalWaitTime >= SHELL_INIT_TIMEOUT_SEC * 1000) {
                return false;
            }
            timeout = Math.min(timeout * 2, (SHELL_INIT_TIMEOUT_SEC * 1000) / 2);
        }
    }

    private void syncOrFail() throws IOException {
        if (!sync()) {
            throw new IOException("Cannot init shell");
        }
    }

    InputStream getInputStream() throws IOException {
        return channelShell.getInputStream();
    }

    OutputStream getOutputStream() throws IOException {
        return channelShell.getOutputStream();
    }

    @Override
    public void close() throws IOException {
        channelShell.disconnect();
        if (shellLogWriter != null) {
            shellLogWriter.close();
        }
    }
}
