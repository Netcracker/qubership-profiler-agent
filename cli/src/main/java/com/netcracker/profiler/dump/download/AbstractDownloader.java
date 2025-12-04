package com.netcracker.profiler.dump.download;

import com.netcracker.profiler.io.Pair;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class AbstractDownloader implements Downloader {
    protected JSch jsch;
    private final String remoteHost;
    private final String userName;
    private final String password;
    protected int parallelDegree;
    protected ExecutorService threadPool;

    public AbstractDownloader(String remoteHost, String userName, String password, int parallelDegree) throws IOException {
        try {
            this.jsch = JSchConfig.createJSch();
        } catch (JSchException e) {
            throw new IOException(e);
        }
        this.remoteHost = remoteHost;
        this.userName = userName;
        this.password = password;
        this.parallelDegree = parallelDegree;
        this.threadPool = Executors.newFixedThreadPool(parallelDegree);
    }

    public DownloadProgress downloadAsync(List<Pair<RemoteFile, File>> remoteAndLocalFiles) throws IOException {
        DownloadProgress progress = new DownloadProgress();
        if (remoteAndLocalFiles.isEmpty()) {
            progress.setCompleted();
            return progress;
        }
        for (Pair<RemoteFile, File> remoteAndLocalFile : remoteAndLocalFiles) {
            RemoteFile remoteFile = remoteAndLocalFile.getKey();
            progress.filesTotal++;
            progress.bytesTotal += remoteFile.length();
        }
        for (Pair<RemoteFile, File> remoteAndLocalFile : remoteAndLocalFiles) {
            RemoteFile remoteFile = remoteAndLocalFile.getKey();
            File localFile = remoteAndLocalFile.getValue();
            localFile.getParentFile().mkdirs();
            DownloadTask downloadTask = new DownloadTask(remoteFile, localFile, progress);
            Future<?> ignore = threadPool.submit(downloadTask);
        }
        return progress;
    }

    @Override
    public void connect() throws IOException {
        createSessionsParallel(remoteHost, userName, password);
    }

    private void createSessionsParallel(String remoteHost, String userName, String password) throws IOException {
        List<Future<CreateConnectionTask>> futures = new ArrayList<>(parallelDegree);
        for (int i = 0; i < parallelDegree; i++) {
            futures.add(threadPool.submit(new CreateConnectionTask(remoteHost, userName, password)));
        }

        for (Future<CreateConnectionTask> future : futures) {
            CreateConnectionTask task = null;
            try {
                task = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            if (task.error != null) {
                throw new IOException(task.error);
            }
        }
    }

    @Override
    public void close() throws IOException {
        threadPool.shutdown();
    }

    protected abstract void downloadFile(String src, String dst) throws IOException;

    protected abstract void createSession(String remoteHost, String userName, String password) throws IOException;

    private class CreateConnectionTask implements Callable<CreateConnectionTask> {
        Throwable error;
        String remoteHost;
        String userName;
        String password;

        CreateConnectionTask(String remoteHost, String userName, String password) {
            this.remoteHost = remoteHost;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public CreateConnectionTask call() throws Exception {
            try {
                createSession(remoteHost, userName, password);
            } catch (Throwable t) {
                error = t;
            }
            return this;
        }
    }

    private class DownloadTask implements Runnable {
        RemoteFile remoteFile;
        File localFile;
        DownloadProgress progress;

        DownloadTask(RemoteFile remoteFile, File localFile, DownloadProgress progress) {
            this.remoteFile = remoteFile;
            this.localFile = localFile;
            this.progress = progress;
        }

        @Override
        public void run() {
            try {
                if (progress.isCanceled()) {
                    throw new CancellationException();
                }
                downloadFile(remoteFile.getPath(), localFile.getAbsolutePath());
                localFile.setLastModified(remoteFile.lastModified());
                progress.filesDownloaded.incrementAndGet();
                progress.bytesDownloaded.addAndGet(remoteFile.length());
            } catch (Throwable t) {
                progress.errors.add(t);
            } finally {
                if ((progress.getFilesDownloaded() + progress.errors.size()) >= progress.getFilesTotal()) {
                    progress.setCompleted();
                }
            }
        }
    }
}
