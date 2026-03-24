package com.netcracker.profiler.sax;

import com.netcracker.profiler.sax.io.IDataInputStreamEx;
import com.netcracker.profiler.sax.visitors.IDictionaryStreamVisitor;
import io.quarkus.logging.Log;

import java.io.IOException;

public class DictionaryPhraseReader  implements IPhraseInputStreamParser {
    private IDataInputStreamEx is;
    private IDictionaryStreamVisitor visitor;


    public DictionaryPhraseReader(IDataInputStreamEx is, IDictionaryStreamVisitor visitor) {
        this.is = is;
        this.visitor = visitor;
    }

    public void parsingPhrases(int len, boolean parseUntilEOF) throws IOException {
        int numberOfBytesToRemain = is.available() - len;

        while (is.available() > numberOfBytesToRemain || parseUntilEOF ) {
            visitor.visitDictionary(is.readString());
        }
        Log.tracef("%d _ %d _ %d", len, is.available(), numberOfBytesToRemain);
    }
}

