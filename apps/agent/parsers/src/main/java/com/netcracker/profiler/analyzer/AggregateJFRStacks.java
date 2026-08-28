package com.netcracker.profiler.analyzer;

import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.sax.stack.DumpVisitor;
import com.netcracker.profiler.sax.stack.DumpsVisitor;
import com.netcracker.profiler.threaddump.parser.ThreadInfo;
import com.netcracker.profiler.threaddump.parser.ThreaddumpParser;
import com.netcracker.profiler.util.ProfilerConstants;

import java.util.ArrayList;

public class AggregateJFRStacks extends DumpsVisitor {
    private final ProfiledTreeStreamVisitor sv;

    private TagDictionary dict = new TagDictionary(100);

    private int dumps;
    private int threads;

    public AggregateJFRStacks(ProfiledTreeStreamVisitor sv) {
        this(ProfilerConstants.PROFILER_V1, sv);
    }

    protected AggregateJFRStacks(int api, ProfiledTreeStreamVisitor sv) {
        super(api);
        this.sv = sv;
    }

    @Override
    public DumpVisitor visitDump() {
        dumps++;
        return new DumpVisitor(ProfilerConstants.PROFILER_V1) {
            ProfiledTree tree = new ProfiledTree(dict, new ClobValues());

            @Override
            public void visitThread(ThreadInfo thread) {
                threads++;
                Hotspot hs = tree.getRoot();
                int j = 0;
                ArrayList<ThreaddumpParser.ThreadLineInfo> trace = thread.stackTrace;
                for (int i = trace.size() - 1; i >= j; i--) {
                    ThreaddumpParser.ThreadLineInfo line = trace.get(i);
                    int id = dict.resolve(line.toString());
                    hs = hs.getOrCreateChild(id);
                    hs.totalTime += thread.value;
                    hs.childTime += thread.value;
                    hs.count++;
                    hs.childCount++;
                }
                hs.childTime -= thread.value;
                hs.childCount--;
            }

            @Override
            public void visitEnd() {
                sv.visitTree(tree);
            }
        };
    }

    @Override
    public void visitEnd() {
        // result.visit(dumps, threads)
        sv.visitEnd();
    }
}
