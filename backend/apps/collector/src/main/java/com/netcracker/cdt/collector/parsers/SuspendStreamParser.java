package com.netcracker.cdt.collector.parsers;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.persistence.op.Operation;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.common.models.meta.SuspendHickup;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.sax.IPhraseInputStreamParser;
import com.netcracker.profiler.sax.SuspendPhraseReader;

import java.util.List;

public final class SuspendStreamParser extends StreamParser<SuspendHickup> {
    SuspendLogParserVisitor visitor;

    public SuspendStreamParser(PodIdRestart pod,
                               SuspendLogParserVisitor visitor,
                               IPhraseInputStreamParser parser,
                               ParsedInputStream parsedInputStream) {
        super(pod, StreamType.SUSPEND, parser, parsedInputStream);
        this.visitor = visitor;
    }

    public static SuspendStreamParser create(PodIdRestart pod) {
        var visitor = new SuspendLogParserVisitor(pod);
        var parsedInputStream = new ParsedInputStream();
        var streamEx = new DataInputStreamEx(parsedInputStream);
        var parser = new SuspendPhraseReader(streamEx, visitor);
        return new SuspendStreamParser(pod, visitor, parser, parsedInputStream);
    }

    @Override
    public List<SuspendHickup> retrieveData() {
        return visitor.getAndClearSuspendHickupList();
    }

    @Override
    public Operation save(PersistenceService service, SuspendHickup toSave) {
        return service.meta.save(toSave);
    }

}
