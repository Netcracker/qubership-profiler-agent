package com.netcracker.cdt.ui.services.tree;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.netcracker.cdt.ui.services.tree.data.ProfiledTree;
import com.netcracker.cdt.ui.services.tree.json.TreeToJson;
import com.netcracker.common.models.meta.ClobIndex;
import com.netcracker.cdt.ui.services.tree.data.GanttInfo;
import com.netcracker.cdt.ui.services.tree.data.Hotspot;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CallTreeMediator {
    public enum DurationFormat {
        TIME,
        BYTES,
        SAMPLES
    }

    private final CallTreeRequest request;

    private DurationFormat durationFormat = DurationFormat.TIME;
    private String mainFileName = "tree.html";

    public CallTreeMediator(CallTreeRequest request) {
        this.request = request;
    }

    public String render(ProfiledTree tree) {
        if (tree == null) {
            Log.error("Should be at least one tree to render");
            return "";
        }
        try {
            ByteArrayBuilder arrayBuilder = new ByteArrayBuilder();
            JsonFactory factory = new JsonFactory();
            JsonGenerator jgen = factory.createGenerator(arrayBuilder);

            String treeVarName = "t";
            TreeToJson converter = new TreeToJson(treeVarName, request.paramTrimSizeForUI());
            jgen.writeRaw(request.callback());
            jgen.writeRaw('(');
            jgen.writeNumber(request.callbackId());
            jgen.writeRaw(", function(){");
            jgen.writeRaw("app.args={}; app.args['params-trim-size']=" + request.paramTrimSizeForUI() + ";\n"); // TODO
            renderArgs(jgen);
            jgen.writeRaw("app.durationFormat='");
            jgen.writeRaw(durationFormat.name());
            jgen.writeRaw("';\n");
            jgen.writeRaw("CT.updateFormatFromPersonalSettings();\n");
            converter.serialize(tree, jgen);

            for (GanttInfo info : tree.ganttInfos) {
                jgen.writeRaw("CT.ganttAppend(");
                jgen.writeNumber(info.startTime);
                jgen.writeRaw(",");
                jgen.writeNumber(info.totalTime);
                jgen.writeRaw(",'");
                jgen.writeRaw(info.fullRow);
                jgen.writeRaw("',");
                jgen.writeNumber(info.folderId);
                jgen.writeRaw(",");
                jgen.writeNumber(info.id);
                jgen.writeRaw(",");
                jgen.writeNumber(info.emit);
                jgen.writeRaw(");");
            }

            Hotspot root = tree.getRoot();
            jgen.writeRaw("CT.timeRange(");
            jgen.writeNumber(root.startTime);
            jgen.writeRaw(",");
            jgen.writeNumber(root.endTime);
            jgen.writeRaw(");");

            jgen.flush();

            addAdjustment(jgen, treeVarName, request.businessCategories(), "CT.defaultCategories", "setCategories");
            addAdjustment(jgen, treeVarName, request.adjustDuration(), "\"\"", "setAdjustments");
            String pageState = request.pageState();
            if (pageState != null && pageState.length() > 0) {
                jgen.writeRaw("$.bbq.pushState($.deparam(");
                jgen.writeString(pageState);
                jgen.writeRaw("));\n");
            }

            jgen.writeRaw("return ");
            jgen.writeRaw(treeVarName);
            jgen.writeRaw(';');
            jgen.writeRaw("})");
            jgen.close();

//            if (tree != null) {
//                renderClobs(tree.getClobValues());
//            }

//            layout.putNextEntry(SinglePageLayout.JAVASCRIPT, mainFileName, "text/javascript");
            return new String(arrayBuilder.toByteArray());
//            layout.getOutputStream().write(result);
//            layout.close();
        } catch (IOException e) {
            Log.errorf(e, "");
        }
        return "";
    }

    private void renderArgs(JsonGenerator jgen) throws IOException {
        for (Map.Entry<String, Object> entry : request.args().entrySet()) {
            jgen.writeRaw("app.args[");
            jgen.writeString(entry.getKey());
            jgen.writeRaw("] = ");
            writeObject(jgen, entry.getValue());
            jgen.writeRaw(";\n");
        }
    }

    private void writeObject(JsonGenerator jgen, Object value) throws IOException {
        if (value instanceof List) {
            jgen.writeStartArray();
            for (var o : (List<?>) value) {
                writeObject(jgen, o);
            }
            jgen.writeEndArray();
        } else if (value instanceof Map) {
            jgen.writeStartObject();
            for (var entry : ((Map<String, Object>) value).entrySet()) {
                jgen.writeFieldName(entry.getKey());
                writeObject(jgen, value);
            }
            jgen.writeEndObject();
        } else {
            jgen.writeObject(value);
        }
    }

    private void renderClobs(ClobIndex clobs) throws IOException {
        for (var clob : clobs.getClobs()) {
            if (clob.isEmpty() || clob.get().length() <= request.paramTrimSizeForUI()) {
                continue;
            }
//            layout.putNextEntry(Layout.CLOB, clob.folder + "/" + clob.fileIndex + "_" + clob.offset + ("sql".equals(clob.folder) ? ".sql" : ".txt"), "text/plain");
//            OutputStream out = layout.getOutputStream();
//            out.write(clob.value.toString().getBytes("UTF-8"));
        }
    }

    private void addAdjustment(JsonGenerator jgen, String treeVarName, String parameterValue, String defaultValue, String methodName) throws IOException {
        jgen.writeRaw("CT." + methodName + "(" + treeVarName + ",");
        if (parameterValue == null || parameterValue.length() == 0)
            jgen.writeRaw(defaultValue);
        else {
            jgen.writeString(parameterValue);
        }
        jgen.writeRaw(");");
    }

}
