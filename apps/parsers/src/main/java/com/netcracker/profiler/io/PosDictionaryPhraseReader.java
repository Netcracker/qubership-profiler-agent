package com.netcracker.profiler.io;

import com.netcracker.profiler.dump.IDataInputStreamEx;

import java.io.IOException;

public class PosDictionaryPhraseReader implements IPhraseInputStreamParser {
    private final IDataInputStreamEx is;
    private final IDictionaryStreamVisitor visitor;

    public PosDictionaryPhraseReader(IDataInputStreamEx is, IDictionaryStreamVisitor visitor) {
        this.is = is;
        this.visitor = visitor;
    }

    public void parsingPhrases(int len, boolean parseUntilEOF) throws IOException {
        int numberOfBytesToRemain = is.available() - len;

        // TODO: check case if we received not enough bytes
        while (is.available() > numberOfBytesToRemain || parseUntilEOF ) {
            visitor.visitDictionary(is.readVarInt(), is.readString());
        }
    }
}
