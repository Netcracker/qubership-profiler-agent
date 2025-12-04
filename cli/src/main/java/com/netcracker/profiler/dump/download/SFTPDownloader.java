package com.netcracker.profiler.dump.download;

import com.netcracker.profiler.io.Pair;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class SFTPDownloader extends AbstractDownloader implements Downloader {
    private ArrayBlockingQueue<Pair<Session, ChannelSftp>> sessionsWithChannels;

    public SFTPDownloader(String remoteHost, String userName, String password, int parallelDegree) throws IOException {
        super(remoteHost, userName, password, parallelDegree);
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
            ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            sessionsWithChannels.add(new Pair<>(session, channelSftp));
        } catch (JSchException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void downloadFile(String src, String dst) throws IOException {
        Pair<Session, ChannelSftp> sessionWithChannel = null;
        try {
            sessionWithChannel = sessionsWithChannels.take();
            ChannelSftp channelSftp = sessionWithChannel.getValue();
            channelSftp.get(src, dst);
        } catch (SftpException e) {
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

    @Override
    public void close() throws IOException {
        super.close();
        for (Pair<Session, ChannelSftp> sessionsWithChannel : sessionsWithChannels) {
            sessionsWithChannel.getValue().disconnect();
            sessionsWithChannel.getKey().disconnect();
        }
    }
}
