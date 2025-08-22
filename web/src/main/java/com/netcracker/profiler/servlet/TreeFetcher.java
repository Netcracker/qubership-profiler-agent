package com.netcracker.profiler.servlet;

import com.netcracker.profiler.config.AnalyzerWhiteList;
import com.netcracker.profiler.fetch.*;
import com.netcracker.profiler.io.CallRowid;
import com.netcracker.profiler.io.FileNameUtils;
import com.netcracker.profiler.output.CallTreeMediator;
import com.netcracker.profiler.output.CallTreeParams;
import com.netcracker.profiler.output.layout.FileAppender;
import com.netcracker.profiler.output.layout.Layout;
import com.netcracker.profiler.output.layout.SinglePageLayout;
import com.netcracker.profiler.output.layout.ZipLayout;
import com.netcracker.profiler.servlet.layout.*;
import com.netcracker.profiler.servlet.layout.ServletLayout;
import com.netcracker.profiler.timeout.ProfilerTimeoutException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class TreeFetcher extends HttpServletBase<CallTreeMediator, TreeFetcher.RequestContext> {
    private static SinglePageLayout.Template template;

    public static final String CHAIN_ID_KEY = "chain";

    static class RequestContext {
        private boolean isZip;
        private int paramsTrimSize;
        private int paramTrimSizeForUI;
    }

    @Override
    protected RequestContext createContext() {
        return new RequestContext();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        ServletContext context = config.getServletContext();
        FileAppender appender = new ServletResourceAppender(context);
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            appender.append("/single-page/tree.html", baos);
            Charset charset = StandardCharsets.UTF_8;
            template = SinglePageLayout.getTemplate(baos.toString(charset.name()), charset);
        } catch (IOException e) {
            throw new ServletException("Unable to read /tree.html template", e);
        }
    }

    @Override
    protected Layout identifyLayout(RequestContext context, HttpServletRequest req, HttpServletResponse resp) {
        Layout l;
        if (!req.getRequestURI().endsWith(".zip")) {
            l = new ServletLayout(resp, "UTF-8", "application/x-javascript");
        } else {
            context.isZip = true;
            l = new ServletLayout(resp, "UTF-8", "application/octet-stream");
            l = new ZipLayout(l);
            l = new SinglePageLayout(l, template);
        }
        int defaultTrimSize = context.isZip ? 200000000 : 15000;
        context.paramsTrimSize = Integer.getInteger("com.netcracker.profiler.Profiler.PARAMS_TRIM_SIZE", defaultTrimSize);
        context.paramTrimSizeForUI = (int) parseLong(req, "params-trim-size", 15000);
        return l;
    }

    @Override
    protected CallTreeMediator getMediator(RequestContext context, HttpServletRequest req, HttpServletResponse resp, Layout layout) {
        String path = req.getRequestURI();
        int extPos = path.lastIndexOf('.');
        String format = extPos == -1 ? "html" : path.substring(extPos + 1);
        String callback = req.getParameter("callback");
        if (callback == null) {
            callback = "html".equals(format) ? "treedata" : "dataReceived";
        }
        String id = req.getParameter("id");
        int callbackId = id == null ? 0 : Integer.valueOf(id);
        CallTreeParams params = new CallTreeParams(req.getParameterMap());

        return new CallTreeMediator(
                layout,
                callback,
                callbackId,
                params,
                context.paramTrimSizeForUI,
                context.isZip
        );
    }

    @Override
    protected Runnable identifyAction(RequestContext context, HttpServletRequest req, HttpServletResponse resp, final CallTreeMediator mediator) {
        final Runnable action = identifyActionInner(context, req, resp, mediator);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    action.run();
                } catch (ProfilerTimeoutException e) {
                    mediator.onError(e);
                }
            }
        };
    }

    private Runnable identifyActionInner(RequestContext context, HttpServletRequest req, HttpServletResponse resp, CallTreeMediator mediator) {
        String dumpsFile = req.getParameter("file");
        String zipName = req.getPathInfo();
        if (zipName == null) {
            zipName = "tree";
        } else {
            zipName = zipName.substring(1, zipName.length() - 4);
        }
        mediator.setMainFileName(zipName);

        if (dumpsFile == null) {
            // Regular profiler's tree

            String[] treeIds = req.getParameterValues("i");
            if (treeIds == null) treeIds = req.getParameterValues("i[]");

            if (treeIds == null) {
                throw new IllegalArgumentException("treeIds should not be null");
            }

            Map<String, Object> args = new HashMap<String, Object>();
            if (context.isZip) {
                args.put("ro", "1");
            }
            args.put("i", Arrays.asList(treeIds));

            final Enumeration paramNames = req.getParameterNames();
            while (paramNames.hasMoreElements()) {
                String param = (String) paramNames.nextElement();
                if (!param.startsWith("f["))
                    continue;
                args.put(param, req.getParameter(param));
            }

            for (Object o : req.getParameterMap().entrySet()) {
                Map.Entry entry = (Map.Entry) o;

                String key = (String) entry.getKey();
                if (key.charAt(0) != 'z') continue;
                String[] value = (String[]) entry.getValue();
                args.put(key, value[0]);
            }

            mediator.mergeArgs(args);

            CallRowid[] callIds = new CallRowid[treeIds.length];
            Map<String, String[]> params = req.getParameterMap();

            for (int i = 0; i < treeIds.length; i++)
                callIds[i] = new CallRowid(treeIds[i], params);

            String[] beginParams = params.get("s");
            String[] endParams = params.get("e");
            long begin = (beginParams != null && beginParams.length == 1) ? Long.parseLong(beginParams[0]) : Long.MIN_VALUE;
            long end = (endParams != null && endParams.length == 1) ? Long.parseLong(endParams[0]) : Long.MAX_VALUE;
            return SpringBootInitializer.fetchCallTreeFactory().fetchCallTree(mediator, callIds, context.paramsTrimSize, begin, end);
        }

        dumpsFile = FileNameUtils.trimFileName(dumpsFile);
        if(!AnalyzerWhiteList.checkAccess(new File(dumpsFile))) {
            mediator.onError(new AccessDeniedException(dumpsFile, null, "Access denied. Edit applications/execution-statistics-collector/config/analyzer_white_list.cfg to grant access."));
            return new Runnable() {public void run() {}};
        }

        AnalyzeSourceFormat format = AnalyzeSourceFormat.AUTO;
        String formatStr = req.getParameter("format");
        if (formatStr != null) {
            format = AnalyzeSourceFormat.valueOf(formatStr.toUpperCase());
        }

        if (format == AnalyzeSourceFormat.AUTO) {
            if (dumpsFile.endsWith(".jfr")) {
                format = AnalyzeSourceFormat.JFR_ALLOCATION;
            } else if (dumpsFile.endsWith(".trc") || dumpsFile.endsWith(".raw")) {
                format = AnalyzeSourceFormat.DBMS_HPROF;
            } else if (dumpsFile.endsWith("collapsed.log") || dumpsFile.endsWith(".aprof.raw")) {
                format = AnalyzeSourceFormat.STACKCOLLAPSE;
            } else {
                format = AnalyzeSourceFormat.THREAD_DUMP;
            }
        }

        if (format == AnalyzeSourceFormat.JFR_ALLOCATION) {
            mediator.setDurationFormat(CallTreeMediator.DurationFormat.BYTES);
            return new FetchJFRAllocations(mediator, dumpsFile);
        }

        if (format == AnalyzeSourceFormat.JFR_CPU) {
            mediator.setDurationFormat(CallTreeMediator.DurationFormat.SAMPLES);
            return new FetchJFRCpu(mediator, dumpsFile);
        }

        if (format == AnalyzeSourceFormat.THREAD_DUMP) {
            mediator.setDurationFormat(CallTreeMediator.DurationFormat.SAMPLES);
            long firstByte = parseLong(req, "firstByte", 0);
            long lastByte = parseLong(req, "lastByte", Long.MAX_VALUE);
            return new FetchThreadDump(mediator, dumpsFile, firstByte, lastByte);
        }

        if (format == AnalyzeSourceFormat.DBMS_HPROF) {
            return new FetchDbmsHprof(mediator, dumpsFile, SpringBootInitializer.getApplicationContext());
        }

        if (format == AnalyzeSourceFormat.STACKCOLLAPSE) {
            mediator.setDurationFormat(CallTreeMediator.DurationFormat.SAMPLES);
            long firstByte = parseLong(req, "firstByte", 0);
            long lastByte = parseLong(req, "lastByte", Long.MAX_VALUE);
            return new FetchStackcollapse(mediator, dumpsFile, firstByte, lastByte);
        }

        throw new IllegalArgumentException("Unexpected output format: " + format);
    }


}
