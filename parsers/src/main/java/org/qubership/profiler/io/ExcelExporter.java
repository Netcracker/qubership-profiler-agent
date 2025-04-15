package org.qubership.profiler.io;

import static org.qubership.profiler.formatters.title.HttpTitleFormatter.*;

import org.qubership.profiler.formatters.title.UrlPatternReplacer;
import org.qubership.profiler.io.xlsx.AggregateCallsToXLSXListener;
import org.qubership.profiler.io.xlsx.CallsToXLSXListener;
import org.qubership.profiler.io.xlsx.ICallsToXLSXListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Component
public class ExcelExporter {
    private static final Logger log = LoggerFactory.getLogger(ExcelExporter.class);
    @Autowired
    ApplicationContext context;
    @Autowired
    CallReaderFactory callReaderFactory;

    public void export(TemporalRequestParams temporal, Map<String, String[]> params, OutputStream out) {
        export(temporal, params, out, "");
    }

    public void export(TemporalRequestParams temporal, Map<String, String[]> params, OutputStream out, String serverAddress) {
        if(serverAddress == null) {
            serverAddress = "";
        }
        String type = getParameterValue("type", params);
        String[] nodesArr = params.get("nodes");
        Set<String> nodes = (nodesArr == null || nodesArr.length == 0) ? null : new HashSet<String>(Arrays.asList(nodesArr));

        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(out, 32768);
        List<Throwable> exceptions = new ArrayList<>();
        CallFilterer cf = new DurationFiltererImpl(temporal.durationFrom, temporal.durationTo);
        ICallsToXLSXListener callListener;
        if("aggregate".equals(type)) {
            String minDigitsInIdStr = getParameterValue("minDigitsInId", params);
            int minDigitsInId = minDigitsInIdStr == null ? 0 : Integer.parseInt(minDigitsInIdStr);
            String[] urlReplacePatternsArr = params.get("urlReplacePatterns");
            UrlPatternReplacer urlPatternReplacer = urlReplacePatternsArr == null ?
                    new UrlPatternReplacer(Collections.EMPTY_LIST) : new UrlPatternReplacer(Arrays.asList(urlReplacePatternsArr));
            String disableDefaultUrlReplacePatternsStr = getParameterValue("disableDefaultUrlReplacePatterns", params);
            boolean disableDefaultUrlReplacePatterns = disableDefaultUrlReplacePatternsStr == null ? false : Boolean.parseBoolean(disableDefaultUrlReplacePatternsStr);

            Map<String, Object> formatContext = new HashMap<>(3);
            formatContext.put(MIN_DIGITS_IN_ID, minDigitsInId);
            formatContext.put(URL_PATTERN_REPLACER, urlPatternReplacer);
            formatContext.put(DISABLE_DEFAULT_URL_REPLACE_PATTERNS, disableDefaultUrlReplacePatterns);

            callListener = context.getBean(AggregateCallsToXLSXListener.class, cf, bufferedOutputStream, formatContext);
        } else {
            callListener = context.getBean(CallsToXLSXListener.class, serverAddress, cf, bufferedOutputStream);
        }
        try {
            List<ICallReader> readers = collectCallReaders(params, callListener, temporal, nodes);
            long start = System.currentTimeMillis();
            for (ICallReader cr : readers) {
                cr.find();
                exceptions.addAll(cr.getExceptions());
            }
            log.info("reading: " + (System.currentTimeMillis() - start));
        } catch (Exception e) {
            exceptions.add(e);
        }
        for(Throwable e: exceptions) {
            callListener.processError(e);
            log.error("Exception when dumping list of calls to Excel: ", e);
        }
        callListener.postProcess();
    }

    private List<ICallReader> collectCallReaders(final Map<String, String[]> params, ICallsToXLSXListener callListener, TemporalRequestParams temporal, Set<String> nodes) throws IOException {
        return callReaderFactory.collectCallReaders(
                params,
                temporal,
                callListener,
                new DurationFiltererImpl(temporal.durationFrom, temporal.durationTo),
                nodes
        );
    }

    private String getParameterValue(String key, Map<String, String[]> params) {
        String[] val = params.get(key);
        if(val == null || val.length == 0) {
            return null;
        }
        return val[0];
    }
}
