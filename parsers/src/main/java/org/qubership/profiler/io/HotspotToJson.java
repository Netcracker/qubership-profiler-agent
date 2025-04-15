package org.qubership.profiler.io;

import org.qubership.profiler.analyzer.comparators.TotalSelfCount;
import org.qubership.profiler.io.serializers.JsonSerializer;
import org.qubership.profiler.sax.values.ClobValue;
import org.qubership.profiler.sax.values.StringValue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class HotspotToJson implements JsonSerializer<Hotspot> {
    private final Map<String, Integer> folder2id;
    private final int maxNestLevel;
    Hotspot rootNode;
    int level;
    int deepIdx;
    ArrayList<StringWriter> deep = new ArrayList<StringWriter>();
    JsonFactory jsonFactory;

    public HotspotToJson(Map<String, Integer> folder2id) {
        this(folder2id, 100); // FF and IE cannot parse highly nested arrays
    }

    public HotspotToJson(Map<String, Integer> folder2id, int maxNestLevel) {
        this.folder2id = folder2id;
        this.maxNestLevel = maxNestLevel;
    }

    public void serialize(Hotspot value, JsonGenerator gen) throws IOException {
        rootNode = value;
        walk(value, gen);
        rootNode = null;
        if (deep.isEmpty()) return;
        gen.writeRaw(';');
        for (int i = 0, deepSize = deep.size(); i < deepSize; i++) {
            StringWriter stringWriter = deep.get(i);
            deep.set(i, null);
            gen.writeRaw(stringWriter.toString());
        }
    }

    protected JsonGenerator createSubSerializer(Writer out) throws IOException {
        if (jsonFactory == null)
            jsonFactory = new JsonFactory();
        return jsonFactory.createGenerator(out);
    }

    private int walk(Hotspot out, JsonGenerator gen) throws IOException {
        int canCollapse = 0;
        gen.writeStartArray();
        gen.writeNumber(out.id);
        gen.writeNumber(out.totalTime);
        gen.writeNumber(out.totalTime - out.childTime);
        gen.writeNumber(out.suspensionTime + out.childSuspensionTime);
        gen.writeNumber(out.suspensionTime);
        gen.writeNumber(out.count);
        gen.writeNumber(out.childCount);
        if (out == rootNode) {
            gen.writeNumber(out.startTime);
            gen.writeNumber(out.endTime);
        } else {
            gen.writeNumber(out.startTime - rootNode.startTime);
            gen.writeNumber(out.endTime - rootNode.startTime);
        }
        gen.writeNumber(out.isReactorFrame);
        gen.writeNumber(out.reactorDuration);
        gen.writeNumber(out.blockingOperator);
        gen.writeNumber(out.reactorStartTime);
        gen.writeNumber(out.reactorLeastTime);
        gen.writeNumber(out.prevOperation);
        gen.writeNumber(out.currentOperation);

        final ArrayList<Hotspot> child = out.children;
        if (child != null) {
            level++;
            JsonGenerator oldOut = gen;
            boolean cut = level > maxNestLevel;
            if (cut) {
                level -= maxNestLevel;
                deepIdx++;
                gen.writeRaw(",d");
                gen.writeRaw(Integer.toString(deepIdx));
                gen.writeRaw("()");
                StringWriter sw = new StringWriter();
                deep.add(sw);
                gen = createSubSerializer(sw);
                gen.writeRaw("\nfunction d");
                gen.writeRaw(Integer.toString(deepIdx));
                gen.writeRaw("(){return");
            }
            gen.writeStartArray();
            if (out.children.size()>1)
                Collections.sort(out.children, TotalSelfCount.INSTANCE);
            final Hotspot firstChild = child.get(0);
            canCollapse = walk(firstChild, gen);
            if (out.tags != null) canCollapse = -2;
            else if ((out.childTime - firstChild.childTime) * 10 <= out.totalTime
                    && (out.totalTime != 0 || (out.childCount - firstChild.childCount) * 10 <= out.childCount)
                    && (out.count == 0 || out.count * 5 > firstChild.count)
                    ){
                    if (canCollapse >= 0) canCollapse++;
                    else canCollapse--;
            } else if (!(out.count == 0 || out.count * 5 > firstChild.count)) canCollapse = -1;
            else canCollapse = canCollapse < 0 ? -3 : 0;

            for (int i = 1; i < child.size(); i++) {
                Hotspot hotspot = child.get(i);
//                    if (hotspot.totalTime<2) continue;
                int canCollapseChild = walk(hotspot, gen);
                if (canCollapseChild < 0 && canCollapse > 0) canCollapse = -3;
            }
            gen.writeEndArray();
            if (cut) {
                gen.writeRaw('}');
                gen.close();
                gen = oldOut;
                level += maxNestLevel;
            }
            gen.writeNumber(canCollapse < -2 ? -3 - canCollapse : (canCollapse > 0 ? canCollapse : 0));
            level--;
        } else if (out.tags != null) {
            gen.writeRaw(",[],0");
        }

        if (out.tags != null) {
            gen.writeStartArray();
            for (HotspotTag tag : out.tags.values()) {
                gen.writeStartArray();
                gen.writeNumber(tag.id);
                gen.writeNumber(tag.totalTime);
                gen.writeNumber(tag.assemblyId);
                gen.writeNumber(tag.isParallel);
                gen.writeStartArray();
                for (Pair<Integer, Integer> parallel : tag.parallels) {
                    gen.writeStartArray();
                    gen.writeNumber(parallel.getKey());
                    gen.writeNumber(parallel.getValue());
                    gen.writeEndArray();
                }
                gen.writeEndArray();
                gen.writeNumber(tag.reactorStartDate);
                gen.writeNumber(tag.count);
                final Object val = tag.value;
                if (val instanceof String) {
                    gen.writeString((String)val);
                } else if (val instanceof StringValue) {
                    gen.writeString(((StringValue)val).value);
                } else if (val instanceof ClobValue) {
                    ClobValue clob = (ClobValue) val;
                    gen.writeRaw(",");
                    gen.writeRaw(clob.folder.charAt(0));
                    gen.writeRaw("[\"");
                    gen.writeRaw(Integer.toString(clob.offset));
                    gen.writeRaw('/');
                    gen.writeRaw(Integer.toString(clob.fileIndex));
                    gen.writeRaw('/');
                    gen.writeRaw(String.valueOf(folder2id.get(clob.dataFolderPath)));
                    gen.writeRaw("\"]");
                } else {
                    gen.writeString("(unknown value type) " + String.valueOf(val));
                }
                gen.writeEndArray();
            }
            gen.writeEndArray();
        }
        gen.writeEndArray();
        if (child == null)
            gen.writeRaw('\n');
        return canCollapse;
    }
}
