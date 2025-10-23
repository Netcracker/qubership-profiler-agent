package com.netcracker.cdt.collector.parsers;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.op.Operation;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.profiler.model.ParameterInfoDto;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.sax.visitors.IParamsStreamVisitor;
import com.netcracker.profiler.sax.IPhraseInputStreamParser;
import com.netcracker.profiler.sax.ParamsPhraseReader;
import com.netcracker.profiler.sax.visitors.ParamsStreamVisitorImpl;

import java.util.List;
import java.util.stream.Collectors;

public final class ParamsStreamParser extends StreamParser<ParamsModel> {
    final IParamsStreamVisitor visitor;

    ParamsStreamParser(PodIdRestart pod,
                       IParamsStreamVisitor visitor,
                       IPhraseInputStreamParser parser,
                       ParsedInputStream parsedInputStream) {
        super(pod, StreamType.PARAMS, parser, parsedInputStream);
        this.visitor = visitor;
    }

    public static ParamsStreamParser create(PodIdRestart pod) {
        var visitor = new ParamsStreamVisitorImpl();
        var parsedInputStream = new ParsedInputStream();
        var streamEx = new DataInputStreamEx(parsedInputStream);
        var parser = new ParamsPhraseReader(streamEx, visitor);
        return new ParamsStreamParser(pod, visitor, parser, parsedInputStream);
    }

    @Override
    public List<ParamsModel> retrieveData() {
        return visitor.getAndCleanParams().stream().map(this::toParamsModel).collect(Collectors.toList());
    }

    ParamsModel toParamsModel(ParameterInfoDto dto) {
        return new ParamsModel(pod, dto.name(), dto.index(), dto.list(), dto.order(), dto.signatureFunction());
    }

    @DB
    public Operation save(PersistenceService service, ParamsModel toSave) {
        return service.meta.save(toSave);
    }
}


