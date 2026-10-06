package com.netcracker.profiler.formatters.title;

import com.netcracker.profiler.agent.ParameterInfo;
import com.netcracker.profiler.agent.ProfilerData;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;

import java.util.*;

public class TitleFormatterFacade {

    private static final ITitleFormatter DEFAULT_FORMATTER = new DefaultTitleFormatter();

    private static Map<String, ITitleFormatter> formatters = new HashMap<>();
    private static List<ParameterInfo> sortedListParams = Collections.EMPTY_LIST;

    static {
        initFormatters();
    }

    private static void initFormatters() {
        ITitleFormatter formatter = new HttpTitleFormatter();
        //netty + spring.webapplicationtype=reactive
        formatters.put("NioEventLoop.runAllTasks", formatter);
        formatters.put("has.url", formatter);
        formatters.put("NioEventLoop.processSelectedKeys", formatter);
        //apache + spring.webapplicationtype=reactive
        formatters.put("SocketProcessorBase.run", formatter);
        //async threads of apache + spring.webapplicationtype=servlet
        formatters.put("TraceRunnable.run", formatter);

        formatters.put("WebAppServletContext$ServletInvocationAction.run", formatter);
        formatters.put("ServletRequestImpl.run", formatter);
        formatters.put("ServletRequestImpl.execute", formatter);
        formatters.put("StandardEngineValve.invoke", formatter);
        formatters.put("FilterHandler$FilterChainImpl.doFilter", formatter);
        formatters.put("Connectors.executeRootHandler", formatter);
        formatters.put("ServletHandler.doHandle", formatter);

        formatter = new QuartzTriggerTitleFormatter();
        formatters.put("NCJobStore.triggerFired", formatter);

        formatter = new QuartzJobTitleFormatter();
        formatters.put("JobRunShell.run", formatter);

        formatter = new JMSTitleFormatter();
        formatters.put("MDListener.run", formatter);
        formatters.put("MDListener.execute", formatter);
        formatters.put("MDListener.onMessage", formatter);
        formatters.put("JMSSession.onMessage", formatter);
        formatters.put("JMSSession$UseForRunnable.run", formatter);
        formatters.put("ClientConsumerImpl.callOnMessage", formatter);
        formatters.put("AbstractMessageListenerContainer.doExecuteListener", formatter);
        formatters.put("ActiveMQMessageHandler.onMessage", formatter);
        formatters.put("JMSMessageListenerWrapper.onMessage", formatter);

        formatter = new DataFlowTitleFormatter();
        formatters.put("DataFlowAwareRunnable.run", formatter);

        formatter = new AutoInstallerTitleFormatter();
        formatters.put("Main.runBuild", formatter);

        formatter = new BrockerTitleFormatter();
        formatters.put("MessagingMessageListenerAdapter", formatter);
    }

    public static void setDefaultListParams(List<ParameterInfo> defaultListParams) {
        Collections.sort(defaultListParams, new Comparator<ParameterInfo>() {
            @Override
            public int compare(ParameterInfo o1, ParameterInfo o2) {
                return Integer.compare(o1.order, o2.order);
            }
        });
        sortedListParams = defaultListParams;
    }

    public static ProfilerTitle formatTitle(int method, TIntObjectHashMap<THashSet<String>> params) {
        String classMethod = asClassMethod(ProfilerData.resolveMethodId(method));
        ITitleFormatter formatter = getFormatter(classMethod, null, params);
        try {
            return formatter.formatTitle(classMethod, params, sortedListParams);
        } catch (Exception e) {
            throw new RuntimeException("Error in formatTile for "+classMethod+", params: "+params, e);
        }
    }

    public static ProfilerTitle formatTitle(String fullClassMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params) {
        String classMethod = asClassMethod(fullClassMethod);
        if(params == null) {
            params = Collections.EMPTY_MAP;
        }
        ITitleFormatter formatter = getFormatter(classMethod, tagToIdMap, params);
        try {
            return formatter.formatTitle(classMethod, tagToIdMap, params, sortedListParams);
        } catch (Exception e) {
            throw new RuntimeException("Error in formatTile for "+classMethod+", params: "+params, e);
        }
    }

    public static ProfilerTitle formatCommonTitle(String fullClassMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        String classMethod = asClassMethod(fullClassMethod);
        if(params == null) {
            params = Collections.EMPTY_MAP;
        }
        ITitleFormatter formatter = getFormatter(classMethod, tagToIdMap, params);
        try {
            return formatter.formatCommonTitle(classMethod, tagToIdMap, params, formatContext);
        } catch (Exception e) {
            throw new RuntimeException("Error in formatTile for "+classMethod+", params: "+params, e);
        }
    }

    private static ITitleFormatter getFormatter(String classMethod, Map<String, Integer> tagToIdMap, Object params) {
        ITitleFormatter formatter = formatters.get(classMethod);
        if(formatter != null) {
            return formatter;
        } else {
            Collection<String> webUrl = TitleCommonTools.getParameterValues(tagToIdMap, params, "web.url");
            Collection<String> queue = TitleCommonTools.getParameterValues(tagToIdMap, params, "queue");
            if(webUrl != null && !webUrl.isEmpty()) {
                return formatters.get("has.url");
            } else if(queue != null && !queue.isEmpty()) {
                return formatters.get("MessagingMessageListenerAdapter");
            } else {
                return DEFAULT_FORMATTER;
            }
        }
    }

    private static String asClassMethod(String methodString) {
        // return-type class-name.method-name(arguments) (source:line) [source-jar]
        int spaceIndex = methodString.indexOf(' ');
        if (spaceIndex != -1) {
            int braceIndex = methodString.indexOf('(', spaceIndex + 1);
            if (braceIndex != -1) {
                int methodDotIndex = methodString.lastIndexOf('.', braceIndex);
                if (methodDotIndex != -1) {
                    int lastClassDot = methodString.lastIndexOf('.', methodDotIndex - 1);
                    return lastClassDot < spaceIndex ? methodString.substring(spaceIndex + 1, braceIndex) : methodString.substring(lastClassDot + 1, braceIndex);
                }
            }
        }
        return methodString;
    }

}
