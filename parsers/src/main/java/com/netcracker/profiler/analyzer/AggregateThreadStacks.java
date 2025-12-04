package com.netcracker.profiler.analyzer;

import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.HotspotTag;
import com.netcracker.profiler.sax.stack.DumpVisitor;
import com.netcracker.profiler.sax.stack.DumpsVisitor;
import com.netcracker.profiler.threaddump.parser.ThreadInfo;
import com.netcracker.profiler.threaddump.parser.ThreaddumpParser;
import com.netcracker.profiler.util.ProfilerConstants;

import java.util.ArrayList;

public class AggregateThreadStacks extends DumpsVisitor {
    private final ProfiledTreeStreamVisitor sv;

    private TagDictionary dict = new TagDictionary(100);
    private int PARAM_THREAD_NAME = dict.resolve("thread.name");

    private int dumps;
    private int threads;
    private boolean groupThreads;

    public AggregateThreadStacks(ProfiledTreeStreamVisitor sv) {
        this(ProfilerConstants.PROFILER_V1, sv);
    }

    public AggregateThreadStacks(ProfiledTreeStreamVisitor sv, boolean groupThreads) {
        this(sv);
        this.groupThreads = groupThreads;
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
                if (groupThreads) {
                    boolean useTid = thread.threadID != null;
                    int id = dict.resolve(useTid ? thread.threadID : thread.name);
                    hs = hs.getOrCreateChild(id);
                    hs.totalTime++;
                    hs.childTime++;
                    if (useTid) {
                        tag(hs, PARAM_THREAD_NAME, thread.name);
                    }
                }
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

    private void tag(Hotspot hs, int tagId, String value) {
        HotspotTag tag = new HotspotTag(tagId);
        tag.addValue(value);
        tag.totalTime = 1;
        hs.addTag(tag);
    }

    @Override
    public void visitEnd() {
        // result.visit(dumps, threads)
        sv.visitEnd();
    }
}
