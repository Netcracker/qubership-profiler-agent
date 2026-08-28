package com.netcracker.profiler.dump.download;

import com.netcracker.profiler.io.Pair;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;

public class SCPDownloader extends AbstractDownloader implements Downloader {
    private ArrayBlockingQueue<Pair<Session, ProfilerChannelShell>> sessionsWithChannels;
    private String shell;

    public SCPDownloader(String remoteHost, String userName, String password, String shell, int parallelDegree) throws IOException {
        super(remoteHost, userName, password, parallelDegree);
        this.shell = shell;
    }

    @Override
    protected void createSession(String remoteHost, String userName, String password) throws IOException {
        try {
            if (sessionsWithChannels == null) {
                sessionsWithChannels = new ArrayBlockingQueue<>(parallelDegree);
            }
            Session session = jsch.getSession(userName, remoteHost);
            JSchConfig.applyToSession(session);
            session.setPassword(password);
            session.connect();
            ProfilerChannelShell channelShell = new ProfilerChannelShell(session, shell, true);
            channelShell.connect();
            sessionsWithChannels.add(new Pair<>(session, channelShell));
        } catch (JSchException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void downloadFile(String src, String dst) throws IOException {
        Pair<Session, ProfilerChannelShell> sessionWithChannel = null;
        try {
            sessionWithChannel = sessionsWithChannels.take();
            downloadFile(sessionWithChannel.getValue(), src, dst);
        } catch (JSchException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sessionWithChannel != null) {
                try {
                    sessionsWithChannels.put(sessionWithChannel);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void downloadFile(ProfilerChannelShell channelShell, String remoteFile, String localFile) throws JSchException, IOException {
        String escapedPath = "'" + remoteFile.replace("'", "'\\''") + "'";
        String command = "scp -fq " + escapedPath;

        channelShell.execCommandNoWait(command);
        // get I/O streams for remote scp
        OutputStream out = channelShell.getOutputStream();
        InputStream in = channelShell.getInputStream();

        byte[] buf = new byte[1024];

        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();

        while (true) {
            int c = checkAck(in, channelShell);
            if (c != 'C') {
                break;
            }

            // read '0644 '
            in.read(buf, 0, 5);

            long filesize = 0L;
            while (true) {
                if (in.read(buf, 0, 1) < 0) {
                    // error
                    break;
                }
                if (buf[0] == ' ') break;
                filesize = filesize * 10L + (long) (buf[0] - '0');
            }

            String file = null;
            for (int i = 0; ; i++) {
                in.read(buf, i, 1);
                if (buf[i] == (byte) 0x0a) {
                    file = new String(buf, 0, i);
                    break;
                }
            }
            //System.out.println("filesize="+filesize+", file="+file);

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();


            try (OutputStream fos = Files.newOutputStream(Paths.get(localFile))) {
                int foo;
                while (true) {
                    if (buf.length < filesize) foo = buf.length;
                    else foo = (int) filesize;
                    foo = in.read(buf, 0, foo);
                    if (foo < 0) {
                        // error
                        break;
                    }
                    fos.write(buf, 0, foo);
                    filesize -= foo;
                    if (filesize == 0L) break;
                }
            }

            if (checkAck(in, channelShell) != 0) {
                return;
            }

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();
        }
    }

    private int checkAck(InputStream in, ProfilerChannelShell channelShell) throws IOException {
        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if (b == 0) return b;
        if (b == -1) return b;

        if (b == 1 || b == 2) { // 1=error, 2=fatal error
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            }
            while (c != '\n');
            channelShell.sendSignal("2"); // sends (CTRL+C) signal
            throw new IOException(sb.toString());
        }
        return b;
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (Pair<Session, ProfilerChannelShell> sessionsWithChannel : sessionsWithChannels) {
            sessionsWithChannel.getValue().close();
            sessionsWithChannel.getKey().disconnect();
        }
    }
}
