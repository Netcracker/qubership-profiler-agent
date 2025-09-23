package com.netcracker.profiler.servlet;


import com.netcracker.profiler.io.*;
import com.netcracker.profiler.servlet.util.DumperStatusProvider;
import com.netcracker.profiler.util.TimeHelper;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CallFetcher extends javax.servlet.http.HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(CallFetcher.class);

    private final static int RESPONSE_BUFFER_SIZE = Integer.getInteger(CallFetcher.class.getName() + ".RESPONSE_BUFFER_SIZE", 256);

//
//    static {
//        try{
//            if(System.getenv(CassandraConfig.CASSANDRA_HOST_ENV) != null) {
//                SpringApplication app = new SpringApplication(CallFetcher.class);
//                app.setWebApplicationType(WebApplicationType.NONE);
//                app.run();
////                SpringApplication.run(CallFetcher.class, new String[]{});
//            }
//        }catch (Throwable e){
//            e.printStackTrace();
//        }
//        log.info("spring boot started");
//    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        boolean isExternalScript = request.getServletPath().endsWith(".js");
        if (isExternalScript){
            response.setBufferSize(RESPONSE_BUFFER_SIZE);
            response.setContentType("application/x-javascript; charset=utf-8");
        }

        final PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "utf-8"), 10240), false);
        TemporalRequestParams temporal = TemporalUtils.parseTemporal(request);

        String callback = request.getParameter("callback");
        if (callback == null) callback = isExternalScript?"dataReceived":"callsdata";
        callback = URLEncoder.encode(callback, "UTF-8");
        out.print(callback);
        out.print('(');
        final String requestId = URLEncoder.encode(request.getParameter("id"), "UTF-8");
        out.print(requestId);
        out.print(", function(){");
        printStartupInfo(requestId, out, temporal);

        List<Throwable> exceptions = new LinkedList<Throwable>();
        try {
            List<ICallReader> readers = collectCallReaders(request, out, temporal);

            out.print("var er=isDump.addProperty(\"");
            out.print(readingFromFileDump());
            out.print("\"");
            out.println(");");

            for (ICallReader cr : readers) {
                cr.find();
                exceptions.addAll(cr.getExceptions());
            }
        } catch (Exception e){
            exceptions.add(e);
            log.error("Failed to read profiler calls: ", e);
        }

        StringBuilder sb = new StringBuilder();
        for(Throwable t: exceptions){
            log.error("Reporting the following exception: ", t);
            sb.append(t.getMessage());
            sb.append("\n");
        }
        if(sb.length() > 0){
            out.print("app.notify.notify('create', 'jqn-error', {title:'Errors occurred',text:\"");
            JSHelper.escapeJS(out, sb.toString());
            out.print("\"}, {expires:false, custom: true});\n");
        }

        out.print("},{timerange:{");
        out.print("min:"); out.print(temporal.timerangeFrom);
        out.print(",max:"); out.print(temporal.timerangeTo);
        out.print(",autoUpdate:"); out.print(temporal.autoUpdate);
        out.print("}, duration:{");
        out.print("min:"); out.print(temporal.durationFrom);
        if (temporal.durationTo!=Long.MAX_VALUE){
            out.print(",max:"); out.print(temporal.durationTo);
        }
        out.print("},availableServices:");
        printAvailableServices(out);
        out.print("});");
        out.flush();
    }

    protected void printStartupInfo(String requestId, PrintWriter out, TemporalRequestParams temporal) throws IOException {
        if ("0".equals(requestId)) {
            String nonLoaded = Installer.getNonActiveProfilerWarning();
            if (nonLoaded != null) {
                out.print("app.notify.notify('create', 'jqn-error', {title:'Profiler is not active',text:\"Profiler gathering agent is not runnig.<br>");
                JSHelper.escapeJS(out, nonLoaded);
                out.print("\"}, {expires:false, custom: true});\n");
            }
            printServerClockWarning(temporal.serverUTC, temporal.clientUTC, out);
            printStartupRequirements(out);
        }
    }

    protected boolean readingFromFileDump(){
        return BooleanUtils.toBoolean(SpringBootInitializer.getIsReadFromDumpProperty());
    }

    protected CallReaderFactory callReaderFactory() {
        return SpringBootInitializer.callReaderFactory();
    }

    protected CallToJS callToJs( PrintWriter out, CallFilterer cf){
        return SpringBootInitializer.callToJs(out, cf);
    }

    private List<ICallReader> collectCallReaders(HttpServletRequest request, PrintWriter out, TemporalRequestParams temporal) throws IOException {
        CallFilterer cf = new DurationFiltererImpl(temporal.durationFrom, temporal.durationTo);
        return callReaderFactory().collectCallReaders(
                request.getParameterMap(),
                temporal,
                callToJs(out, cf),
                cf
        );

//        String searchConditionsStr = request.getParameter("searchConditions");
//        if(!StringUtils.isBlank(searchConditionsStr) && !"undefined".equalsIgnoreCase(searchConditionsStr)){
//            SearchConditions conditions = new SearchConditions(searchConditionsStr, new Date(temporal.timerangeFrom), new Date(temporal.timerangeTo));
//            for(LoadRequest loadRequest: conditions.loadRequests()){
//                CallReader cr = CallReader.newReader(new CallToJS(out), filterByDuration(temporal.durationFrom, temporal.durationTo), loadRequest.getPodName());
//                long clientServerTimeDiff = (long) (Math.abs(temporal.clientUTC - temporal.serverUTC) * 1.5);
//                cr.setTimeFilterCondition(loadRequest.getDateFrom().getTime() - clientServerTimeDiff, loadRequest.getDateTo().getTime() + clientServerTimeDiff);
//                readers.add(cr);
//            }
//        } else {
//            CallReader cr = CallReader.newReader(new CallToJS(out), filterByDuration(temporal.durationFrom, temporal.durationTo), null);
//            long clientServerTimeDiff = (long) (Math.abs(temporal.clientUTC - temporal.serverUTC) * 1.5);
//            cr.setTimeFilterCondition(temporal.timerangeFrom - clientServerTimeDiff, temporal.timerangeTo + clientServerTimeDiff);
//            readers.add(cr);
//        }
//
//        return readers;
    }

    private void printStartupRequirements(PrintWriter out) throws IOException {
        //do not provide recommendation when running as a separate cloud service
        DumperStatusProvider dumper = DumperStatusProvider.INSTANCE;
        dumper.update();
        if(!dumper.isStarted) {
            return;
        }

        List<String> recommendations = Installer.getStartupArgumentRecommendations();
        for (String recommendation : recommendations) {
            out.print("app.notify.notify('create', 'jqn-error', {title:'Startup arguments misconfiguration',text:\"");
            JSHelper.escapeJS(out, recommendation);
            out.print("\"}, {expires: 10000, custom: true});\n");
        }
    }

    protected List<String[]> getAvailableServices() {
        return SpringBootInitializer.loggedContainersInfo().listPodDetails();
    }

    private void printAvailableServices(PrintWriter out){
        List<String[]> podDetails = getAvailableServices();
        if(podDetails == null){
            out.write("null");
            return ;
        }
        out.write("{");

        printList(out, "podNames", podDetails, 0);

        out.write(",");
        printList(out, "serviceNames", podDetails, 1);

        out.write(",");

        printList(out, "namespaces", podDetails, 2);

        out.write("}");

    }

    private void printList(PrintWriter out, String listName, List<String[]> list, int index){
        Set<String> distinctList = new TreeSet<String>();
        for(String[] src : list){
            distinctList.add(src[index]);
        }
        out.write(listName);
        out.write(":[");
        for(Iterator<String> it=distinctList.iterator(); it.hasNext();){
            out.write("\"");
            out.write(it.next());
            out.write("\"");
            if(it.hasNext()) out.write(",");
        }
        out.write("]");
    }

    private void printServerClockWarning(long serverUTC, long clientUTC, PrintWriter out) {
        long diffInSeconds = Math.abs(serverUTC - clientUTC) / 1000;
        //it takes about 8 seconds to initialize spring during application first call
        if (diffInSeconds <= 10) return;
        out.print("app.notify.notify('create', 'jqn-error', {title:'Server clock might be wrong',text:\"Looks like server clock is ");
        out.print(TimeHelper.humanizeDifference(null, serverUTC - clientUTC));
        out.print("<br>Take this difference into consideration when applying timerange filters.<br>Ask IT to enable NTP time synchronization on both client and server\"}, {expires:false, custom: true});\n");
    }
}
