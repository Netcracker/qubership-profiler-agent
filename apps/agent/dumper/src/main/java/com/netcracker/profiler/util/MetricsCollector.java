package com.netcracker.profiler.util;

import static com.netcracker.profiler.agent.FilterOperator.*;
import static com.netcracker.profiler.agent.FilterOperator.ADDITIONAL_INPUT_PARAMS;

import com.netcracker.profiler.ServerNameResolver;
import com.netcracker.profiler.agent.*;
import com.netcracker.profiler.configuration.MetricsConfigurationImpl;
import com.netcracker.profiler.dump.ThreadState;
import com.netcracker.profiler.metrics.AggregationParameter;
import com.netcracker.profiler.metrics.MetricsPluginImpl;

import gnu.trove.set.hash.THashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MetricsCollector {
    private final static Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

    private static BitSet matchedCalls = new BitSet();
    private static BitSet unmatchedCalls = new BitSet();

    private static Map<String, Object> params = new HashMap<String, Object>(4);
    private static Map<String, String> additionalInputParams = new HashMap<String, String>(2);

    static {
        additionalInputParams.put(NODE_NAME_PARAM, String.valueOf(ServerNameResolver.SERVER_NAME));
        params.put(ADDITIONAL_INPUT_PARAMS, additionalInputParams);
    }

    public static void collectMetrics(
            MetricsPluginImpl metricsPlugin,
            ThreadState threadState,
            List<MetricsConfiguration> metricsConfiguration,
            long callDuration,
            CallInfo callInfo,
            ThreadState thread,
            String threadName) {
        try {
            if (unmatchedCalls.get(threadState.method)) {
                return;
            }

            params.put(CALL_INFO_PARAM, callInfo);
            params.put(THREAD_STATE_PARAM, threadState);
            params.put(DURATION_PARAM, callDuration);
            additionalInputParams.put(THREAD_NAME_PARAM, threadName);

            for (MetricsConfiguration metricConfig : metricsConfiguration) {
                if (matchedCalls.get(threadState.method) || checkClassMethodMatching(threadState, metricConfig)) {
                    if (((MetricsConfigurationImpl) metricConfig).getFilter().evaluate(params) && !metricConfig.isCustom()) {
                        HashSet<AggregationParameter> aggregationParameters = new HashSet<AggregationParameter>();
                        for (AggregationParameterDescriptor aggregationParameterDescriptor : metricConfig.getAggregationParameters()) {
                            aggregationParameters.add(
                                    new AggregationParameter(
                                            aggregationParameterDescriptor.getDisplayName(),
                                            ("user".equals(aggregationParameterDescriptor.getName())) ? new THashSet<String>(Collections.singleton(callInfo.getNcUser())) : getThreadStateParameter(threadState, aggregationParameterDescriptor.getName())
                                    ));
                        }

                        for (MetricsDescription metricsDescription : metricConfig.getMetrics()) {
                            MetricType metricType = MetricType.getByConfigName(metricsDescription.getName());
                            if (metricType != null) {
                                metricsPlugin.getOrCreateMetric(metricType, metricConfig.getName(), aggregationParameters, metricConfig.isCustom(),
                                        metricsDescription.getParameters(), metricConfig.getOutputVersion()).
                                        recordValue(callDuration, params);
                            } else {
                                LOG.error("No MetricType found by metric description: {}", metricsDescription.getName());
                            }
                        }
                    }
                }
            }

            if (!matchedCalls.get(threadState.method)) {
                unmatchedCalls.set(threadState.method);
            }
        } catch (Exception ex) {
            LOG.error("Error in collectMetrics", ex);
        }
    }

    public static void resetCaches() {
        matchedCalls.clear();
        unmatchedCalls.clear();
    }

    private static boolean checkClassMethodMatching(ThreadState threadState, MetricsConfiguration metricConfig) {
        // Example:
        // java.sql.ResultSet weblogic.jdbc.wrapper.Statement.executeQuery(java.lang.String) (Statement.java:497) [mw1036\/modules\/com.bea.core.datasource6_1.10.0.0.jar]
        String callName = ProfilerData.resolveMethodId(threadState.method);

        // weblogic.jdbc.wrapper.Statement.executeQuery(java.lang.String) (Statement.java:497) [mw1036\/modules\/com.bea.core.datasource6_1.10.0.0.jar]
        callName = callName.substring(callName.indexOf(' ') + 1);

        // weblogic.jdbc.wrapper.Statement.executeQuery
        callName = callName.substring(0, callName.indexOf('('));

        // weblogic.jdbc.wrapper.Statement
        String className = callName.substring(0, callName.lastIndexOf('.'));

        // executeQuery
        String methodName = callName.substring(callName.lastIndexOf('.') + 1);

        if (className.equals(metricConfig.getMatchingClass()) && ("".equals(metricConfig.getMatchingMethod()) || methodName.equals(metricConfig.getMatchingMethod()))) {
            matchedCalls.set(threadState.method);
            return true;
        } else {
            return false;
        }
    }

    private static THashSet<String> getThreadStateParameter(ThreadState threadState, String parameterName) {
        THashSet<String> parameterValues = threadState.params.get(ProfilerData.resolveTag(parameterName));

        if(parameterValues == null){
            return new THashSet<>();
        }else {
            return new THashSet<>(parameterValues);
        }
    }

}
