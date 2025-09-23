package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.io.serializers.JsonSerializer;
import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.sax.values.StringValue;
import com.netcracker.profiler.util.ThrowableHelper;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


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
        Map<Hotspot, List<Hotspot>> calculateList = new HashMap<>();
        if (children == null) {
            hs2js.serialize(new Hotspot(0), gen);
        } else if (children.size() == 1) {
            transformTree(root, root, getAllParents(root), calculateList, new AtomicInteger());
            remap(calculateList);
        } else {
            Map<Long, Hotspot> allParents = getAllParents(root);
            transformTree(root, root, allParents, calculateList, new AtomicInteger());
            remap(calculateList);

            List<Hotspot> collect = collectSorted(calculateList.values());

            for (Hotspot hotspot : collect) {
                long reactorStartTime = hotspot.reactorStartTime;
                long reactorLeastTime = reactorStartTime + hotspot.reactorDuration;
                if (hotspot.isReactorFrame != 0) {
                    for (Hotspot hp : collect) {
                        if (hp.isReactorFrame != 0) {
                            long hpReactorStartTime = hp.reactorStartTime + 100;
                            long hpReactorLeastTime = hp.reactorStartTime + hp.reactorDuration - 100;

                            boolean start = hpReactorStartTime >= reactorStartTime
                                    && hpReactorStartTime <= reactorLeastTime;

                            boolean end = hpReactorLeastTime >= reactorStartTime
                                    && hpReactorLeastTime <= reactorLeastTime;

                            boolean between = hpReactorStartTime <= reactorStartTime
                                    && hpReactorLeastTime >= reactorLeastTime;

                            for (HotspotTag ht : hotspot.tags.values()) {
                                if (ht.value instanceof StringValue && StringUtils.isNumeric(ht.value.toString())) {
                                    if ((start || end || between) && hp.reactorCallId != hotspot.reactorCallId) {
                                        ht.parallels.add(Pair.of(hp.id, hp.reactorDuration));
                                        ht.isParallel = 1;
                                    }
                                    ht.reactorStartDate = hotspot.reactorStartTime;
                                }
                            }
                        }
                    }
                }
                calculate(root, hotspot.reactorCallId, hotspot, hotspot.id, false);
            }

            for (Hotspot hotspot : calculateList.keySet()) {
                Map<Integer, Hotspot> reactorIds = new HashMap<>();
                Iterator<Hotspot> iterator = hotspot.children.iterator();
                while (iterator.hasNext()) {
                    Hotspot child = iterator.next();
                    if (reactorIds.containsKey(child.id)) {
                        Hotspot hp = reactorIds.get(child.id);
                        hp.mergeWithChildren(child);
                        iterator.remove();
                    } else {
                        reactorIds.put(child.id, child);
                    }
                }
            }
        }
        long totalTime = 0;
        if(children != null) {
            for (Hotspot child : children) {
                totalTime += child.totalTime;
                if (child.totalTime < child.reactorDuration) {
                    totalTime += child.reactorDuration;
                    root.reactorDuration += child.reactorDuration;
                }
            }
        }
        root.totalTime = root.childTime = totalTime;
        hs2js.serialize(root, gen);
    }

    private void remap(Map<Hotspot, List<Hotspot>> calculateList) {
        for (Map.Entry<Hotspot, List<Hotspot>> hotspotListEntry : calculateList.entrySet()) {
            Hotspot parent = hotspotListEntry.getKey();
            List<Hotspot> child = hotspotListEntry.getValue();
            if (parent.children == null) {
                parent.children = new ArrayList<>();
            }
            parent.children.addAll(child);
        }
    }


    private void transformTree(Hotspot root,
                               Hotspot mainRoot,
                               Map<Long, Hotspot> transform,
                               Map<Hotspot, List<Hotspot>> calculateMap,
                               AtomicInteger counter) {
        ArrayList<Hotspot> children = root.children;
        Iterator<Hotspot> iterator = children.iterator();
        while (iterator.hasNext()) {
            try {
                Hotspot child = iterator.next();
                if (child.children != null) {
                    transformTree(child, mainRoot, transform, calculateMap, counter);
                }
                if (child.lastParentAssemblyId != 0 && calculateMap != Collections.EMPTY_MAP) {
                    Hotspot hotspotLast = transform.get(child.lastParentAssemblyId);
                    if (hotspotLast != null
                            && (hotspotLast.children == null || !hotspotLast.children.containsAll(root.children))
                            && !hotspotLast.lastAssemblyId.contains(root.lastParentAssemblyId)) {
                        child.reactorCallId = counter.incrementAndGet();
                        if (root != mainRoot) {
                            calculate(mainRoot, child.reactorCallId, child, child.id, true);
                        }
                        iterator.remove();
                        if (!calculateMap.containsKey(hotspotLast)) {
                            ArrayList<Hotspot> value = new ArrayList<>();
                            calculateMap.put(hotspotLast, value);
                            value.add(child);
                        } else {
                            calculateMap.get(hotspotLast).add(child);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Can't transform current child");
            }
        }
        if (root.children.isEmpty()) {
            root.children = null;
        }
    }

    private Hotspot calculate(Hotspot hotspot, int id, Hotspot child, int methodId, boolean isClean) {
        if (hotspot.children == null) {
            return null;
        }

        for (Hotspot c : hotspot.children) {
            if (id == c.reactorCallId && methodId == c.id) {
                if (isClean) {
                    clean(hotspot, child);
                } else {
                    merge(hotspot, child);
                }
                return child;
            }

            Hotspot h = calculate(c, id, child, methodId, isClean);

            if (h != null) {
                if (isClean) {
                    clean(hotspot, h);
                } else {
                    merge(hotspot, h);
                }
                return h;
            }
        }

        return null;
    }

    private void clean(Hotspot hotspot, Hotspot h) {
        hotspot.childTime -= h.totalTime;
        hotspot.totalTime -= h.totalTime;
        hotspot.childCount -= h.count + h.childCount;
        hotspot.childSuspensionTime -= h.suspensionTime + h.childSuspensionTime;
    }

    private void merge(Hotspot hotspot, Hotspot h) {
        if (hotspot.reactorStartTime != 0 && h.reactorStartTime != 0) {
            hotspot.reactorStartTime = Math.min(h.reactorStartTime, hotspot.reactorStartTime);
        } else if (h.reactorStartTime != 0) {
            if (hotspot.reactorDuration != 0) {
                hotspot.reactorStartTime = h.reactorStartTime;
            } else {
                hotspot.reactorStartTime = hotspot.startTime + hotspot.totalTime;
                hotspot.reactorLeastTime = hotspot.startTime + hotspot.totalTime;
            }
        }

        if (h.reactorStartTime != 0) {
            int prevReactorDuration = hotspot.reactorDuration;
            h.reactorLeastTime = h.reactorStartTime + h.reactorDuration;
            hotspot.reactorLeastTime = Math.max(hotspot.reactorLeastTime, h.reactorStartTime + h.reactorDuration);
            hotspot.reactorDuration = (int) (hotspot.reactorLeastTime - hotspot.reactorStartTime);
            hotspot.childTime = hotspot.childTime - prevReactorDuration + hotspot.reactorDuration;
            hotspot.totalTime = hotspot.totalTime - prevReactorDuration + hotspot.reactorDuration;
        } else {
            hotspot.childTime += (h.totalTime - h.reactorDuration);
            hotspot.totalTime += (h.totalTime - h.reactorDuration);
        }

        hotspot.childCount += h.count + h.childCount;
        hotspot.childSuspensionTime += h.suspensionTime + h.childSuspensionTime;

        h.childTime -= h.childSuspensionTime;
        h.totalTime -= h.childSuspensionTime + h.suspensionTime;

        if (hotspot.tags != null) {
            Iterator<HotspotTag> iterator = hotspot.tags.values().iterator();
            while (iterator.hasNext()) {
                HotspotTag value = iterator.next();
                if (value.assemblyId == h.lastParentAssemblyId) {
                    iterator.remove();
                    value.totalTime = h.totalTime + h.reactorDuration;
                    if (h.tags == null) {
                        h.tags = new HashMap<>();
                    }
                    h.tags.put(value, value);
                }
            }
        }
    }

    /**
     * collect an index of lastAssemblyId -> hotspot
     * for all hotspots with lastAssemblyId != lastParentAssemblyId
     * @param root
     * @return
     */
    private Map<Long, Hotspot> getAllParents(Hotspot root) {
        Map<Long, Hotspot> stringListHashMap = new HashMap<>();
        if (root.children != null) {
            for (Hotspot child : root.children) {
                if (child.lastAssemblyId != null) {
                    for (Long aLong : child.lastAssemblyId) {
                        if(aLong != child.lastParentAssemblyId) stringListHashMap.put(aLong, child);
                    }
                }
                Map<Long, Hotspot> allParents = getAllParents(child);
                if (!allParents.isEmpty()) {
                    stringListHashMap.putAll(allParents);
                }
            }
        }
        return stringListHashMap;
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
