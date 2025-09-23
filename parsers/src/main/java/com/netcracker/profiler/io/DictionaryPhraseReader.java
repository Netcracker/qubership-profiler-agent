package com.netcracker.profiler.io;

import com.netcracker.profiler.dump.IDataInputStreamEx;

import java.io.IOException;

public class DictionaryPhraseReader implements IPhraseInputStreamParser {
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
    }
}
