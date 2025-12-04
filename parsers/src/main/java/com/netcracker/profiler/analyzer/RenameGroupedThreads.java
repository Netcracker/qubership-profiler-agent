package com.netcracker.profiler.analyzer;

import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.HotspotTag;
import com.netcracker.profiler.util.ProfilerConstants;

public class RenameGroupedThreads extends ProfiledTreeStreamVisitor {
    public RenameGroupedThreads(ProfiledTreeStreamVisitor tv) {
        this(ProfilerConstants.PROFILER_V1, tv);
    }

    protected RenameGroupedThreads(int api, ProfiledTreeStreamVisitor tv) {
        super(api, tv);
    }

    @Override
    public void visitTree(ProfiledTree tree) {
        TagDictionary dict = tree.getDict();
        int paramThreadName = dict.resolve("thread.name");
        if(tree.getRoot().children != null) {
            for (Hotspot hs : tree.getRoot().children) {
                if(hs.tags == null) {
                    continue;
                }
                String tid = dict.resolve(hs.id);
                int threadNamesCount = 0;
                long maxDuration = 0;
                String maxDurationThreadName = null;
                for (HotspotTag hst : hs.tags.values()) {
                    if(hst.id != paramThreadName) {
                        continue;
                    }
                    threadNamesCount++;
                    if(hst.totalTime > maxDuration) {
                        maxDuration = hst.totalTime;
                        maxDurationThreadName = (String) hst.value;
                    }
                }
                if(maxDurationThreadName != null) {
                    dict.put(hs.id, maxDurationThreadName + " (TID: "+tid+")" + (threadNamesCount > 1 ? " (Unique thread names: "+threadNamesCount +")" : ""));
                }
            }
        }
        super.visitTree(tree);
    }
}
