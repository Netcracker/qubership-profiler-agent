package com.netcracker.profiler.sax.readers;

import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.sax.raw.ClobReaderFlyweight;
import com.netcracker.profiler.sax.raw.ClobReaderFlyweightFile;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.sax.raw.TraceVisitor;
import com.netcracker.profiler.sax.raw.TreeRowid;
import com.netcracker.profiler.sax.raw.TreeTraceVisitor;
import com.netcracker.profiler.sax.readers.ProfilerTraceReader.ClobReadMode;
import com.netcracker.profiler.sax.readers.ProfilerTraceReader.ClobReadTypes;
import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.sax.values.ValueHolder;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

class MockTreeTraceVisitor extends TreeTraceVisitor {
    public static ArrayList<HashMap<String, String>> visitList;
    public HashMap<String, String> visit = new HashMap<String, String>();
    public boolean visitExitVisited = false;
    public boolean visitLabelVisited = false;
    public MockTreeTraceVisitor(int api, TreeTraceVisitor tv) {
        super(api, tv);
    }

    @Override
    public void visitEnter(int methodId){
        visit.put("methodId", Integer.toString(methodId));
        visitList.add(visit);
    }

    @Override
    public void visitExit() {
        visitExitVisited = true;
    }

    @Override
    public void visitLabel(int labelId, ValueHolder value) {
        visitLabelVisited = true;
    }
}

class MockProfilerTraceReader extends ProfilerTraceReader {
    public String dataFolder;
    public MockProfilerTraceReader(RepositoryVisitor rv, String rootReference) {
        super(rv, rootReference);
    }

    public File getFile() {
        File file = null;
        try {
            file = ResourceUtils.getFile("classpath:storage/test_trace");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return file;
    }

    @Override
    public DataInputStreamEx reopenDataInputStream(DataInputStreamEx oldOne, String streamName, int traceFileIndex)
            throws IOException {
        return DataInputStreamEx.reopenDataInputStream(oldOne, getFile(), streamName, traceFileIndex);
    }

    @Override
    public ClobReaderFlyweight clobReaderFlyweight() {
        ClobReaderFlyweightFile result = new ClobReaderFlyweightFile();
            result.setDataFolder(getFile());
        return result;
    }
}

public class ProfilerTraceReaderTest {
    @Test
    public void testLastOnlyClobs() throws IOException {
        File file = ResourceUtils.getFile("classpath:storage/test_trace/trace/000001");
        Set<ClobValue> clobs = MockProfilerTraceReader.readClobIdsOnly(file, ClobReadMode.LAST_ONLY, ClobReadTypes.ALL_VALUES);
        List<ClobValue> clobArray = new ArrayList<ClobValue>(clobs);
        ClobValue clob = clobArray.get(0);
        assertEquals(1, clobArray.size());
        assertEquals("xml", clob.folder);
        assertEquals(513630, clob.offset);
        assertEquals(null, clob.value);
    }

    @Test
    public void testFirstAndLastClobs() throws IOException {

        File file = ResourceUtils.getFile("classpath:storage/test_trace/trace/000001");
        Set<ClobValue> clobs = MockProfilerTraceReader.readClobIdsOnly(file, ClobReadMode.FIRST_AND_LAST, ClobReadTypes.ALL_VALUES);
        List<ClobValue> clobArray = new ArrayList<ClobValue>(clobs);
        Collections.sort(clobArray);
        assertEquals(2, clobArray.size());
        ClobValue clob = clobArray.get(0);
        assertEquals("sql", clob.folder);
        assertEquals(0, clob.offset);
        assertEquals(null, clob.value);
        clob = clobArray.get(1);
        assertEquals("xml", clob.folder);
        assertEquals(513630, clob.offset);
        assertEquals(null, clob.value);

    }

    @Test
    public void testFirstOnlyClobs() throws IOException {
        File file = ResourceUtils.getFile("classpath:storage/test_trace/trace/000001");
        Set<ClobValue> clobs = MockProfilerTraceReader.readClobIdsOnly(file, ClobReadMode.FIRST_ONLY, ClobReadTypes.ALL_VALUES);
        List<ClobValue> clobArray = new ArrayList<ClobValue>(clobs);
        assertEquals(1, clobArray.size());
        ClobValue clob = clobArray.get(0);
        assertEquals("sql", clob.folder);
        assertEquals(0, clob.offset);
        assertEquals(null, clob.value);
    }

    @Test
    public void testAllClobs() throws IOException {
        File file = ResourceUtils.getFile("classpath:storage/test_trace");
        Set<ClobValue> clobs = MockProfilerTraceReader.readClobIdsOnly(file, ClobReadMode.ALL_VALUES, ClobReadTypes.ALL_VALUES);
        assertEquals(0, clobs.size());
    }

    @Test
    public void testReadTracesOne() throws IOException {
        List<TreeRowid> treeRowids = new ArrayList<TreeRowid>();
        treeRowids.add(new TreeRowid(2, "1_1", 1, 997, 0));
        TraceVisitor tv = Mockito.spy(new TraceVisitor(0));
        RepositoryVisitor rv  = Mockito.spy(new RepositoryVisitor(0));

        MockProfilerTraceReader reader = new MockProfilerTraceReader(rv, "storage/test_trace");
        Mockito.when(rv.visitTrace()).thenReturn(tv);
        MockTreeTraceVisitor result = new MockTreeTraceVisitor(0, new TreeTraceVisitor(0));
        MockTreeTraceVisitor.visitList = new ArrayList<HashMap<String, String>>();
        Mockito.when(tv.visitTree(treeRowids.get(0))).thenReturn(result);

        reader.read(treeRowids);
        String expected = "[{methodId=174}, {methodId=174}, {methodId=174}]";
        assertEquals(expected, MockTreeTraceVisitor.visitList.toString());
        assertTrue(result.visitExitVisited);
        assertFalse(result.visitLabelVisited);
    }

    @Test
    public void testReadTracesTwo() throws IOException {
        List<TreeRowid> treeRowids = new ArrayList<TreeRowid>();
        treeRowids.add(new TreeRowid(3, "3_3", 1, 1815, 0));
        TraceVisitor tv = Mockito.spy(new TraceVisitor(0));
        RepositoryVisitor rv  = Mockito.spy(new RepositoryVisitor(0));

        MockProfilerTraceReader reader = new MockProfilerTraceReader(rv, "storage/test_trace");
        Mockito.when(rv.visitTrace()).thenReturn(tv);
        MockTreeTraceVisitor result = new MockTreeTraceVisitor(0, new TreeTraceVisitor(0));
        MockTreeTraceVisitor.visitList = new ArrayList<HashMap<String, String>>();
        Mockito.when(tv.visitTree(treeRowids.get(0))).thenReturn(result);

        reader.read(treeRowids);
        String expected = "[{methodId=173}, {methodId=173}]";
        assertEquals(expected, MockTreeTraceVisitor.visitList.toString());
        assertTrue(result.visitExitVisited);
        assertFalse(result.visitLabelVisited);
    }

    @Test
    public void testReadTracesThree() throws IOException {
        List<TreeRowid> treeRowids = new ArrayList<TreeRowid>();
        treeRowids.add(new TreeRowid(4, "4_1", 1, 3172, 0));
        TraceVisitor tv = Mockito.spy(new TraceVisitor(0));
        RepositoryVisitor rv  = Mockito.spy(new RepositoryVisitor(0));

        MockProfilerTraceReader reader = new MockProfilerTraceReader(rv, "storage/test_trace");
        Mockito.when(rv.visitTrace()).thenReturn(tv);
        MockTreeTraceVisitor result = new MockTreeTraceVisitor(0, new TreeTraceVisitor(0));
        MockTreeTraceVisitor.visitList = new ArrayList<HashMap<String, String>>();
        Mockito.when(tv.visitTree(treeRowids.get(0))).thenReturn(result);

        reader.read(treeRowids);
        String expected = "[{methodId=593}]";
        assertEquals(expected, MockTreeTraceVisitor.visitList.toString());
        assertTrue(result.visitExitVisited);
        assertFalse(result.visitLabelVisited);
    }
}
