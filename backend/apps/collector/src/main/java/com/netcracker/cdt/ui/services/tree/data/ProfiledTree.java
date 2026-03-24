package com.netcracker.cdt.ui.services.tree.data;

import com.netcracker.common.models.meta.ClobIndex;
import com.netcracker.common.models.meta.DictionaryIndex;
import com.netcracker.profiler.model.TreeRowId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProfiledTree {
    private Hotspot root = new Hotspot(-1);
    public List<GanttInfo> ganttInfos = new ArrayList<>();
    private DictionaryIndex dict;
    private ClobIndex clobValues;
    private boolean ownDict = false;
    private TreeRowId rowid = TreeRowId.UNDEFINED;

    public ProfiledTree(DictionaryIndex dict, ClobIndex clobValues) {
        this.dict = dict;
        this.clobValues = clobValues;
    }

    public ProfiledTree(DictionaryIndex dict, ClobIndex clobValues, TreeRowId rowid) {
        this(dict, clobValues);
        this.rowid = rowid;
    }

    public Hotspot getRoot() {
        return root;
    }

    public DictionaryIndex getDict() {
        return dict;
    }

    public ClobIndex getClobValues() {
        return clobValues;
    }

    public TreeRowId getRowid() {
        return rowid;
    }

    public void merge(ProfiledTree that) {
        if (dict != that.dict && !ownDict) {
            ownDict = true;
            dict = dict.clone();
        }
        if (!that.clobValues.getClobs().isEmpty()) {
            clobValues.merge(that.clobValues);
        }
        Map<Integer, Integer> remapIds = dict.mergeForRemap(that.dict);
        that.root.remap(remapIds);

        if (root.id != that.root.id)
            throw new IllegalArgumentException("Unable to merge two trees with different root ids: " + root.id + " and " + that.root.id);
        root.mergeWithChildren(that.root, ganttInfos);
        rowid = TreeRowId.UNDEFINED;
    }
}
