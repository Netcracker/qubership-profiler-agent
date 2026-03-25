package com.netcracker.cdt.collector.parsers;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.persistence.op.Operation;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.sax.DictionaryPhraseReader;
import com.netcracker.profiler.sax.visitors.DictionaryStreamVisitorImpl;
import com.netcracker.profiler.sax.visitors.IDictionaryStreamVisitor;
import com.netcracker.profiler.sax.IPhraseInputStreamParser;

import java.util.ArrayList;
import java.util.List;

public final class DictionaryStreamParser extends StreamParser<DictionaryModel> {
    final IDictionaryStreamVisitor visitor;
    int lastKnownPosition;

    DictionaryStreamParser(PodIdRestart pod,
                           int lastKnownPosition,
                           IDictionaryStreamVisitor visitor,
                           IPhraseInputStreamParser parser,
                           ParsedInputStream parsedInputStream) {
        super(pod,  StreamType.DICTIONARY, parser, parsedInputStream);
        this.visitor = visitor;
        this.lastKnownPosition = lastKnownPosition;
    }

    public static DictionaryStreamParser create(PodIdRestart pod, int lastKnownPosition) {
        var visitor = new DictionaryStreamVisitorImpl();
        var parsedInputStream = new ParsedInputStream();
        var streamEx = new DataInputStreamEx(parsedInputStream);
        var parser = new DictionaryPhraseReader(streamEx, visitor);
        return new DictionaryStreamParser(pod, lastKnownPosition, visitor, parser, parsedInputStream);
    }

    static DictionaryModel newDictionary(PodIdRestart pod, Integer position, String tag) {
        return new DictionaryModel(pod, position, tag);
    }

    @Override
    public List<DictionaryModel> retrieveData() {
        return toModels(visitor.getAndCleanDictionary());
    }

    List<DictionaryModel> toModels(List<String> dictionary) {
        List<DictionaryModel> result = new ArrayList<>(dictionary.size());
//        loadLastKnownPosition();

        for (String dict : dictionary) {
            result.add(newDictionary(pod, ++lastKnownPosition, dict));
        }
        return result;
    }

//    @Override
//    public void resetExistingContents() {
//        Log.infof("resetExistingContents %s", podName);
//        streamDictionaryRepository.resetExistingContents(podName);
//    }


    @Override
    public Operation save(PersistenceService service, DictionaryModel toSave) {
        return service.meta.save(toSave);
    }
}
