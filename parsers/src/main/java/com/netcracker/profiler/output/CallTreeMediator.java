package com.netcracker.profiler.output;

import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.dom.GanttInfo;
import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.TreeToJson;
import com.netcracker.profiler.io.exceptions.ErrorCollector;
import com.netcracker.profiler.io.exceptions.ErrorMessage;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.output.layout.Layout;
import com.netcracker.profiler.output.layout.SinglePageLayout;
import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.util.ProfilerConstants;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CallTreeMediator extends ProfiledTreeStreamVisitor {
    private static final Logger log = LoggerFactory.getLogger(CallTreeMediator.class);
    public enum DurationFormat {
        TIME,
        BYTES,
        SAMPLES
    }

    private final Layout layout;
    private ProfiledTree tree;
    private DurationFormat durationFormat = DurationFormat.TIME;
    private final String callback;
    private String mainFileName = "tree.html";
    private final int callbackId;
    private final Map<String, String> folderIdMapping;

    private final CallTreeParams params;
    private final int paramTrimSizeForUI;
    private final boolean isZip;
    private final Map<String, Object> args = new HashMap<String, Object>();

    public CallTreeMediator(Layout layout,
                            String callback,
                            int callbackId,
                            CallTreeParams params,
                            int paramTrimSizeForUI,
                            boolean isZip,
                            Map<String, String> folderIdMapping) {
        super(ProfilerConstants.PROFILER_V1);
        this.layout = layout;
        this.callback = callback;
        this.callbackId = callbackId;
        this.params = params;
        this.paramTrimSizeForUI = paramTrimSizeForUI;
        this.isZip = isZip;
        this.folderIdMapping = folderIdMapping;
    }

    public void setDurationFormat(DurationFormat durationFormat) {
        this.durationFormat = durationFormat;
    }

    public void mergeArgs(Map<String, Object> args) {
        this.args.putAll(args);
    }

    @Override
    public void visitTree(ProfiledTree tree) {
        if (this.tree != null)
            throw new IllegalStateException("Call tree is not supposed to render more than one tree at once");
        this.tree = tree;
    }

    public void setMainFileName(String mainFileName) {
        this.mainFileName = mainFileName;
    }

    @Override
    public void visitEnd() {
        try {
            ByteArrayBuilder arrayBuilder = new ByteArrayBuilder();
            JsonFactory factory = new JsonFactory();
            JsonGenerator jgen = factory.createGenerator(arrayBuilder);

            if (tree == null) {
                ErrorSupervisor.getInstance().error("Should be at least one tree to render", new IllegalStateException("Should be at least one tree to render"));
                printErrors(jgen);
                jgen.writeRaw("document.getElementById('loading').style.display='none';");
            } else {
                String treeVarName = "t";
                TreeToJson converter = new TreeToJson(treeVarName, paramTrimSizeForUI);
                jgen.writeRaw(callback);
                jgen.writeRaw('(');
                jgen.writeNumber(callbackId);
                jgen.writeRaw(", function(){");
                jgen.writeRaw("app.args={}; app.args['params-trim-size']=" + paramTrimSizeForUI + ";\n"); // TODO
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

                initGanttFolders(jgen);
                jgen.flush();

                PrintWriter writer = new PrintWriter(arrayBuilder);
                writer.flush();

                printErrors(jgen);

                addAdjustment(jgen, treeVarName, "businessCategories", "CT.defaultCategories", "setCategories");
                addAdjustment(jgen, treeVarName, "adjustDuration", "\"\"", "setAdjustments");
                String pageState = params.get("pageState");
                if (pageState != null && pageState.length() > 0) {
                    jgen.writeRaw("$.bbq.pushState($.deparam(");
                    jgen.writeString(pageState);
                    jgen.writeRaw("));\n");
                }

                jgen.writeRaw("return ");
                jgen.writeRaw(treeVarName);
                jgen.writeRaw(';');
                jgen.writeRaw("})");
            }

            jgen.close();
            layout.putNextEntry(SinglePageLayout.JAVASCRIPT, mainFileName + ".html", "text/javascript");
            byte[] result = arrayBuilder.toByteArray();
            layout.getOutputStream().write(result);
            if (tree != null) {
                renderClobs(tree.getClobValues());
            }
            layout.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            String message;
            if(isZip) {
                String errorClass = t.getClass().getSimpleName();
                layout.putNextEntry(SinglePageLayout.HTML, errorClass + ".html", "text/html");
                message = t.getMessage();
            } else {
                message = "alert('"+t.getMessage()+"');";
            }

            try(OutputStreamWriter writer = new OutputStreamWriter(layout.getOutputStream())) {
                writer.write(message);
            }
        } catch (IOException ex) {
            log.error("", ex);
        }
    }

    private void printErrors(JsonGenerator jgen) throws IOException {
        ErrorCollector collector = ErrorSupervisor.findFirst(ErrorCollector.class);
        if (collector == null) {
            String msg = "ErrorSupervisor is expected to have ErrorCollector. Will not be able to show exceptions to client";
            ErrorSupervisor.getInstance().warn(msg, new IllegalStateException(msg));
        } else {
            List<ErrorMessage> errors = collector.getErrors();
            for (ErrorMessage error : errors) {
                error.toJson(jgen);
            }
        }
    }

    private void initGanttFolders(JsonGenerator jgen) throws IOException {
        for(Map.Entry<String, String> entry: folderIdMapping.entrySet()) {
            jgen.writeRaw("CT.addGanttFolder(");
            jgen.writeRaw(entry.getKey());  //numeric. should not enclose in quotes
            jgen.writeRaw(",'");
            jgen.writeRaw(StringUtils.replace(StringUtils.replace(entry.getValue(), "'", "\\'"), "\n", "\\n")); //just in case escape js literal
            jgen.writeRaw("');");
        }
    }

    private void renderArgs(JsonGenerator jgen) throws IOException {
        for (Map.Entry<String, Object> entry : args.entrySet()) {
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
            List list = (List) value;
            for (Object o : list) {
                writeObject(jgen, o);
            }
            jgen.writeEndArray();
        } else if (value instanceof Map) {
            jgen.writeStartObject();
            Map<String, Object> map = (Map<String, Object>) value;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                jgen.writeFieldName(entry.getKey());
                writeObject(jgen, value);
            }
            jgen.writeEndObject();
        } else {
            jgen.writeObject(value);
        }
    }

    private void renderClobs(ClobValues clobs) throws IOException {
        for (ClobValue clob : clobs.getClobs()) {
            if (clob.value == null || clob.value.length() <= paramTrimSizeForUI) {
                continue;
            }
            layout.putNextEntry(Layout.CLOB, clob.folder + "/" + clob.fileIndex + "_" + clob.offset + ("sql".equals(clob.folder) ? ".sql" : ".txt"), "text/plain");
            OutputStream out = layout.getOutputStream();
            out.write(clob.value.toString().getBytes("UTF-8"));
        }
    }

    private void addAdjustment(JsonGenerator jgen, String treeVarName, String parameterName, String defaultValue, String methodName) throws IOException {
        String value = params.get(parameterName);
        jgen.writeRaw("CT." + methodName + "(" + treeVarName + ",");
        if (value == null || value.length() == 0)
            jgen.writeRaw(defaultValue);
        else {
            jgen.writeString(value);
        }
        jgen.writeRaw(");");
    }

}
