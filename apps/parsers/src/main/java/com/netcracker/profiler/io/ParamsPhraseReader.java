package com.netcracker.profiler.io;

import com.netcracker.profiler.dump.IDataInputStreamEx;

import java.io.IOException;

public class ParamsPhraseReader implements IPhraseInputStreamParser {
    private IDataInputStreamEx is;
    private IParamsStreamVisitor visitor;
    private int version;


    public ParamsPhraseReader(IDataInputStreamEx is, IParamsStreamVisitor visitor) {
        this.is = is;
        this.visitor = visitor;
    }

    private void initVersion() throws IOException {
        if (version == 0) {
            version = is.read();

        }
    }

    public void parsingPhrases(int lenOfPhraseToRead, boolean parseUntilEOF) throws IOException {
        int numberOfBytesToRemain = is.available() - lenOfPhraseToRead;

        initVersion();

        while (is.available() > numberOfBytesToRemain || parseUntilEOF) {
            String name = is.readString();

            boolean index = is.read() == 1;
            boolean list = is.read() == 1;
            int order = is.readVarInt();
            String signature = is.readString();

            visitor.visitParam(name, list, index, order, signature);
        }

    }
}
