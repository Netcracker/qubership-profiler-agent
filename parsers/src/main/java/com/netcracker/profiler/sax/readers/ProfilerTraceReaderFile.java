package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.ParamReader;
import com.netcracker.profiler.io.ParamReaderFactory;
import com.netcracker.profiler.sax.raw.ClobReaderFlyweight;
import com.netcracker.profiler.sax.raw.ClobReaderFlyweightFile;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.sax.raw.SuspendLogVisitor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
@Scope("prototype")
@Profile("filestorage")
public class ProfilerTraceReaderFile extends ProfilerTraceReader {

    @Value("${com.netcracker.profiler.DUMP_ROOT_LOCATION}")
    File dumpRoot;

    @Autowired
    ParamReaderFactory paramReaderFactory;

    @Autowired
    ApplicationContext context;

    public ProfilerTraceReaderFile() {
        super();
//        throw new RuntimeException("No-args not supported");
    }

    private File dataFolder(){
        return new File(dumpRoot, rootReference);
    }

    public ProfilerTraceReaderFile(RepositoryVisitor rv, String rootReference) {
        super(rv, rootReference);
    }

    protected SuspendLogReader suspendLogReader(SuspendLogVisitor sv, long begin, long end) {
        return context.getBean(SuspendLogReader.class, sv, dataFolder().getAbsolutePath(), begin, end);
    }

    protected SuspendLogReader suspendLogReader(SuspendLogVisitor sv) {
        return suspendLogReader(sv, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public DataInputStreamEx reopenDataInputStream(DataInputStreamEx oldOne, String streamName, int traceFileIndex) throws IOException {
        return DataInputStreamEx.reopenDataInputStream(oldOne, dataFolder(), streamName, traceFileIndex);
    }

    @Override
    public ClobReaderFlyweight clobReaderFlyweight() {
        ClobReaderFlyweightFile result = new ClobReaderFlyweightFile();
        result.setDataFolder(dataFolder());
        return result;
    }

    protected ParamReader paramReader(){
        return paramReaderFactory.getInstance(dataFolder().getAbsolutePath());
    }
}
