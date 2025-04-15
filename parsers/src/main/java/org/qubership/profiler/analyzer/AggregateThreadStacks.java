package org.qubership.profiler.analyzer;

import org.qubership.profiler.dom.ClobValues;
import org.qubership.profiler.dom.ProfiledTree;
import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.dom.TagDictionary;
import org.qubership.profiler.io.Hotspot;
import org.qubership.profiler.sax.stack.DumpVisitor;
import org.qubership.profiler.sax.stack.DumpsVisitor;
import org.qubership.profiler.threaddump.parser.ThreadInfo;
import org.qubership.profiler.threaddump.parser.ThreaddumpParser;
import org.qubership.profiler.util.ProfilerConstants;

import java.util.ArrayList;

public class AggregateThreadStacks extends DumpsVisitor {
    private final ProfiledTreeStreamVisitor sv;

    private TagDictionary dict = new TagDictionary(100);

    private int dumps;
    private int threads;

    public AggregateThreadStacks(ProfiledTreeStreamVisitor sv) {
        this(ProfilerConstants.PROFILER_V1, sv);
    }

    protected AggregateThreadStacks(int api, ProfiledTreeStreamVisitor sv) {
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
                    hs.totalTime++;
                    hs.childTime++;
                }
                hs.childTime--;
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
