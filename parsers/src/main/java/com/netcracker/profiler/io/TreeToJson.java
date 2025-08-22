package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.io.serializers.JsonSerializer;
import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.util.ThrowableHelper;

import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TreeToJson implements JsonSerializer<ProfiledTree> {
    protected final String treeVarName;
    private final int paramTrimSizeForUI;
    private static final Logger log = LoggerFactory.getLogger(TreeToJson.class);

    public TreeToJson(String treeVarName, int paramTrimSizeForUI) {
        this.treeVarName = treeVarName;
        this.paramTrimSizeForUI = paramTrimSizeForUI;
    }

    public void serialize(ProfiledTree tree, JsonGenerator gen) throws IOException {
        gen.writeRaw("var S=CT.sqls, B=CT.xmls;\n");
        gen.writeRaw("var " + treeVarName + ";\n");
        if (tree == null) return;
        try {
            renderTags(tree.getDict(), gen);
            Map<String, Integer> folder2id = renderClobs(tree.getClobValues(), gen);
            gen.writeRaw(treeVarName + " = ");
            renderCallTree(tree, gen, folder2id);
            gen.writeRaw(";\n");
            gen.writeRaw(treeVarName + " = CT.append(" + treeVarName + ", []);\n");
        } catch (Throwable t) {
            log.error("", t);
            gen.writeRaw(ThrowableHelper.throwableToString(t));
//            handleException(t);
        }
    }

    private List<Hotspot> collectSorted(Collection<List<Hotspot>> listOfLists) {
        List<Hotspot> collect = new ArrayList<>();
        for(List<Hotspot> toCollect: listOfLists){
            collect.addAll(toCollect);
        }
        Collections.sort(collect, new Comparator<Hotspot>() {
            @Override
            public int compare(Hotspot o1, Hotspot o2) {
                return Long.compare(o1.startTime, o2.startTime);
            }
        });
        return collect;
    }

    private void renderCallTree(ProfiledTree agg, JsonGenerator gen, Map<String, Integer> folder2id) throws IOException {
        JsonSerializer<Hotspot> hs2js = new HotspotToJson(folder2id);
        Hotspot root = agg.getRoot();
        final ArrayList<Hotspot> children = root.children;
        if (children == null) {
            hs2js.serialize(new Hotspot(0), gen);
        } else if (children.size() == 1) {
            hs2js.serialize(children.get(0), gen);
        } else {
            long totalTime = 0;
            for (Hotspot child : children)
                totalTime += child.totalTime;
            root.totalTime = root.childTime = totalTime;
            hs2js.serialize(root, gen);
        }
    }

    private Map<String, Integer> renderClobs(ClobValues clobs, JsonGenerator gen) throws IOException {
        Map<String, Integer> folder2id = new HashMap<String, Integer>();
        gen.writeRaw("var s={}; var x={}; var tc;\n");
        for (ClobValue clob : clobs.getClobs()) {
            Integer folderId = folder2id.get(clob.dataFolderPath);
            if (folderId == null) {
                folderId = folder2id.size();
                folder2id.put(clob.dataFolderPath, folderId);
            }
            gen.writeRaw("tc=");
            gen.writeRaw(clob.folder.charAt(0));
            gen.writeRaw('[');
            gen.writeString(clob.offset + "/" + clob.fileIndex + "/" + folderId);
            gen.writeRaw("]=");
            CharSequence value = clob.value;
            boolean stringIsBig = false;
            if (value != null && value.length() >= paramTrimSizeForUI) {
                value = value.subSequence(0, paramTrimSizeForUI);
                stringIsBig = true;
            }
            if (stringIsBig) {
                gen.writeRaw("new String(");
            }
            gen.writeString(String.valueOf(value));
            if (stringIsBig) {
                gen.writeRaw(")");
            }
            gen.writeRaw(";\n");
            if (stringIsBig) {
                gen.writeRaw("tc._0=");
                gen.writeNumber(clob.fileIndex);
                gen.writeRaw(";\n");
                gen.writeRaw("tc._1=");
                gen.writeNumber(clob.offset);
                gen.writeRaw(";\n");
                gen.writeRaw("tc._2=");
                gen.writeString(clob.folder);
                gen.writeRaw(";\n");
            }
        }
        return folder2id;
    }

    private void renderTags(TagDictionary dict, JsonGenerator gen) throws IOException {
        final List<String> tags = dict.getTags();
        final BitSet requredIds = dict.getIds();
        int k = 0;
        gen.writeRaw("t=CT.tags;");
        for (int i = -1; (i = requredIds.nextSetBit(i + 1)) >= 0; ) {
            final String tag = tags.get(i);
            if (tag == null) continue;
            gen.writeRaw("t.a(");
            gen.writeNumber(i);
            gen.writeRaw(',');
            gen.writeString(tag);
            gen.writeRaw(");");
            k++;
            if (k == 10) {
                gen.writeRaw('\n');
                k = 0;
            }
        }

        k = 0;
        for (ParameterInfoDto info : dict.getParamInfo().values()) {
            gen.writeRaw("t.b(");
            gen.writeString(info.name);
            gen.writeRaw(',');
            gen.writeRaw(info.list ? '1' : '0');
            gen.writeRaw(',');
            gen.writeNumber(info.order);
            gen.writeRaw(',');
            gen.writeRaw(info.index ? '1' : '0');
            gen.writeRaw(',');
            if (info.signatureFunction == null)
                gen.writeString("");
            else
                gen.writeString(info.signatureFunction);
            gen.writeRaw(");");
            k++;
            if (k == 10) {
                gen.writeRaw('\n');
                k = 0;
            }
        }
        if (k != 0)
            gen.writeRaw('\n');
    }
}
