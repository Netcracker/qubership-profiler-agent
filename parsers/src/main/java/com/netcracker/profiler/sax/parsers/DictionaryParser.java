package com.netcracker.profiler.sax.parsers;

import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.sax.raw.DictionaryVisitor;

import java.util.ArrayList;

public class DictionaryParser {
    public void parse(DictionaryVisitor dv, TagDictionary dict) {
        // TODO: optimize for DictionaryBuilder (share dict)
        ArrayList<String> tags = dict.getTags();
        for (int i = 0; i < tags.size(); i++) {
            String s = tags.get(i);
            if (s == null)
                continue;
            dv.visitName(i, s);
        }
    }
}
