package com.netcracker.profiler.io;

import java.io.IOException;

public interface IPhraseInputStreamParser {

    void parsingPhrases(int lenOfPhraseToRead, boolean parseUntilEOF) throws IOException;
}
