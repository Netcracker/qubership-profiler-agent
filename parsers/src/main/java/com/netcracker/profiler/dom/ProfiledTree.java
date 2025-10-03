package com.netcracker.profiler.dom;

import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.sax.raw.TreeRowid;

import java.util.Map;

public class ProfiledTree {
    private Hotspot root = new Hotspot(-1);
    private TagDictionary dict;
    private ClobValues clobValues;
    private boolean ownDict = false;
    private TreeRowid rowid = TreeRowid.UNDEFINED;

    public ProfiledTree(TagDictionary dict, ClobValues clobValues) {
        this.dict = dict;
        this.clobValues = clobValues;
    }

    public ProfiledTree(TagDictionary dict, ClobValues clobValues, TreeRowid rowid) {
        this(dict, clobValues);
        this.rowid = rowid;
    }

    public Hotspot getRoot() {
        return root;
    }

    public TagDictionary getDict() {
        return dict;
    }

    public ClobValues getClobValues() {
        return clobValues;
    }

    public TreeRowid getRowid() {
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
        Map<Integer, Integer> remapIds = dict.merge(that.dict);

        that.root.remap(remapIds);

        if (root.id != that.root.id)
            throw new IllegalArgumentException("Unable to merge two trees with different root ids: " + root.id + " and " + that.root.id);
        root.mergeWithChildren(that.root);

        rowid = TreeRowid.UNDEFINED;
    }
}
