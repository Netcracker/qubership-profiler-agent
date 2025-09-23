package com.netcracker.profiler.test.dump;

import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.dump.*;
import com.netcracker.profiler.io.listener.FileRotatedListener;

import mockit.Expectations;
import mockit.internal.reflection.MethodReflection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.*;

public class DumpFileManagerTest {

    @TempDir
    private Path rootDir;
    private String rootPath;
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getIntegerInstance();

    static {
        NUMBER_FORMAT.setGroupingUsed(false);
        NUMBER_FORMAT.setMinimumIntegerDigits(6);
    }

    @BeforeEach
    public void prepareDumpRoot() throws IOException {
        rootPath = rootDir.toString();
    }

    @Test
    public void fileLoads() throws URISyntaxException {
        File flist = new File(getClass().getResource("/oom_filelist_without_dependent_files.blst").toURI());
        if (flist.length() != 36864) {
            return;
        }
        try (DumpFileLog log = new DumpFileLog(flist)) {
            Queue<DumpFile> dumpFiles = log.parseIfPresent();
            assertNull(dumpFiles, "dumpQueue should be null since the file is corrupted");
        }
    }

    @Test
    public void testLoadByFinder() {
        final DumpFile dumpFile = new DumpFile(
                rootPath + "2015/03/11/1426083986775/calls/000001.gz"
                , 0L
                , 0L
        );
        new Expectations() {
            {
                new FileDeleter(); // one instance is expected
                // no method invocations

                try(DumpFileLog dumpFileLog = new DumpFileLog(new File(rootPath, DumpFileLog.DEFAULT_NAME))) { // one instance is expected
                    dumpFileLog.cleanup(null, true);
                    times = 1;

                    DumpFilesFinder dumpFilesFinder = new DumpFilesFinder(); // one instance creation is expected

                    dumpFilesFinder.search(rootPath);
                    result = new LinkedList<DumpFile>(Arrays.asList(dumpFile));

                    dumpFileLog.writeAddition(dumpFile);
                    times = 1;
                }
            }
        };

        try(DumpFileManager dumpFileManager = new DumpFileManager(
                0L /* no age limits */
                , 0L /* no size limits */
                , rootPath
                , true /* force scan files in root */
        )){}
    }

    @Test
    public void testLoadFromFile() {
        final Queue<DumpFile> expected = new LinkedList<DumpFile>();
        for (int i = 0; i < 10; i++) {
            DumpFile file = new DumpFile(
                    String.format(rootPath + "2015/03/11/1426083986775/calls/" + NUMBER_FORMAT.format(i + 1) + ".gz")
                    , 0L
                    , 0L
            );
            expected.add(file);
        }

        new Expectations() {
            {
                new FileDeleter(); // one instance is expected
                // no method invocations

                try(DumpFileLog dumpFileLog = new DumpFileLog(new File(rootPath, DumpFileLog.DEFAULT_NAME))) { // one instance is expected
                    dumpFileLog.parseIfPresent();
                    result = expected;
                }
            }
        };

        try( DumpFileManager dumpFileManager = new DumpFileManager(
                0L /* no age limits */
                , 0L /* no size limits */
                , rootPath
                , false /* read from file */
        )){};

    }

    @Test
    public void testLoadFromFileWithDeletion() {
        long maxAge = 50000L;
        long currentTime = System.currentTimeMillis();
        currentTime = currentTime - (currentTime % 1000);
        final long borderTime = currentTime - maxAge;
        final Queue<DumpFile> expected = new LinkedList<DumpFile>();
        for (int i = 0; i < 10; i++) {
            DumpFile file = new DumpFile(
                    String.format(rootPath + "2015/03/11/1426083986775/calls/" + NUMBER_FORMAT.format(i + 1) + ".gz")
                    , 0L
                    , borderTime - 50000L - 500L + 10000L * (i + 1)
            );
            expected.add(file);
        }
        final List<DumpFile> expectedList = new ArrayList<DumpFile>(expected);

        final long dumpCurrentTime = currentTime;
        new Expectations() {
            {
                FileDeleter fileDeleter = new FileDeleter(); // one instance is expected

                try(DumpFileLog dumpFileLog = new DumpFileLog(new File(rootPath, DumpFileLog.DEFAULT_NAME))) { // one instance is expected
                    dumpFileLog.parseIfPresent();
                    result = expected;

                    MethodReflection.invokePublicIfAvailable(System.class, (Object) null, "currentTimeMillis", new Class[]{});
                    result = dumpCurrentTime;
                    {
                        DumpFile dumpFile = expectedList.get(0);
                        // we expect 5 files to be deleted
                        fileDeleter.deleteFile(dumpFile);
                        result = true;
                        dumpFileLog.writeDeletion(dumpFile);
                    }
                    {
                        DumpFile dumpFile = expectedList.get(1);
                        // we expect 5 files to be deleted
                        fileDeleter.deleteFile(dumpFile);
                        result = true;
                        dumpFileLog.writeDeletion(dumpFile);
                    }
                    {
                        DumpFile dumpFile = expectedList.get(2);
                        // we expect 5 files to be deleted
                        fileDeleter.deleteFile(dumpFile);
                        result = true;
                        dumpFileLog.writeDeletion(dumpFile);
                    }
                    {
                        DumpFile dumpFile = expectedList.get(3);
                        // we expect 5 files to be deleted
                        fileDeleter.deleteFile(dumpFile);
                        result = true;
                        dumpFileLog.writeDeletion(dumpFile);
                    }
                    {
                        DumpFile dumpFile = expectedList.get(4);
                        // we expect 5 files to be deleted
                        fileDeleter.deleteFile(dumpFile);
                        result = true;
                        dumpFileLog.writeDeletion(dumpFile);
                    }
                }
            }
        };

        try(DumpFileManager dumpFileManager = new DumpFileManager(
                maxAge /* 5000ms */
                , 0L /* no size limits */
                , rootPath
                , false /* read from file */
        )) {}
    }


    @Test
    public void testFileRotatedWithDeletion() {
        final List<DumpFile> expected = new LinkedList<DumpFile>();
        for (int i = 0; i < 10; i++) {
            DumpFile file = new DumpFile(
                    String.format(rootPath + "2015/03/11/1426083986775/calls/" + NUMBER_FORMAT.format(i + 1) + ".gz")
                    , 10000L
                    , 0L
            );
            expected.add(file);
        }

        new Expectations() {
            {
                FileDeleter fileDeleter = new FileDeleter(); // one instance is expected

                try (DumpFileLog dumpFileLog = new DumpFileLog(new File(rootPath, DumpFileLog.DEFAULT_NAME))) { // one instance is expected
                    dumpFileLog.parseIfPresent();
                    result = new LinkedList<DumpFile>();

                    dumpFileLog.writeAddition(expected.get(0));
                    dumpFileLog.writeAddition(expected.get(1));
                    dumpFileLog.writeAddition(expected.get(2));
                    dumpFileLog.writeAddition(expected.get(3));
                    dumpFileLog.writeAddition(expected.get(4));

                    {
                        dumpFileLog.writeAddition(expected.get(5));
                        DumpFile deletedFile = expected.get(0);
                        fileDeleter.deleteFile(deletedFile);
                        result = true;
                        dumpFileLog.writeDeletion(deletedFile);
                    }
                    {
                        dumpFileLog.writeAddition(expected.get(6));
                        DumpFile deletedFile = expected.get(1);
                        fileDeleter.deleteFile(deletedFile);
                        result = true;
                        dumpFileLog.writeDeletion(deletedFile);
                    }
                    {
                        dumpFileLog.writeAddition(expected.get(7));
                        DumpFile deletedFile = expected.get(2);
                        fileDeleter.deleteFile(deletedFile);
                        result = true;
                        dumpFileLog.writeDeletion(deletedFile);
                    }
                    {
                        dumpFileLog.writeAddition(expected.get(8));
                        DumpFile deletedFile = expected.get(3);
                        fileDeleter.deleteFile(deletedFile);
                        result = true;
                        dumpFileLog.writeDeletion(deletedFile);
                    }
                    {
                        dumpFileLog.writeAddition(expected.get(9));
                        DumpFile deletedFile = expected.get(4);
                        fileDeleter.deleteFile(deletedFile);
                        result = true;
                        dumpFileLog.writeDeletion(deletedFile);
                    }
                }
            }
        };

        try(DumpFileManager dumpFileManager = new DumpFileManager(
                0L /* no age limits */
                , 50000L /* 20K */
                , rootPath
                , false /* read from file */
        )) {
            FileRotatedListener listener = dumpFileManager.getFileRotatedListener();
            for (DumpFile dumpFile : expected) {
                listener.fileRotated(dumpFile, null);
            }
        }
    }
}
