package com.netcracker.profiler.metrics;

import com.netcracker.profiler.agent.*;
import com.netcracker.profiler.util.StringUtils;

import gnu.trove.set.hash.THashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsPluginImpl implements MetricsPlugin {

    private static final int INTERVAL_TO_SHOW_METRIC = 1000 * 60 * 60;
    private final static Logger LOG = LoggerFactory.getLogger(MetricsPluginImpl.class);

    private ConcurrentHashMap<MetricsKey, Metric> callMetrics = new ConcurrentHashMap<MetricsKey, Metric>();
    private HashSet<SystemMetric> systemMetrics = new HashSet<>();

    public MetricsPluginImpl() {
        Bootstrap.registerPlugin(MetricsPlugin.class, this);
    }

    public Metric getMetric(
            MetricType metricType,
            String callType,
            Map<String, String> aggregationParameters) {
        HashSet<AggregationParameter> aggregationParameterHashSet = new HashSet<AggregationParameter>();
        for (Map.Entry<String, String> aggregationParameter : aggregationParameters.entrySet()) {
            THashSet<String> aggregationParamValues = new THashSet<String>();
            aggregationParamValues.add(aggregationParameter.getValue());
            aggregationParameterHashSet.add(new AggregationParameter(aggregationParameter.getKey(), aggregationParamValues));
        }

        MetricsConfiguration metricsConfiguration = Profiler.getMetricConfigByName(callType);
        if (metricsConfiguration == null) {
            LOG.error("MetricsConfiguration not found for callType: {}", callType);
            return null;
        }

        for (MetricsDescription metricsDescription : metricsConfiguration.getMetrics()) {
            if (metricType.getConfigName().equals(metricsDescription.getName())) {
                return getOrCreateMetric(
                        metricType,
                        callType,
                        aggregationParameterHashSet,
                        metricsConfiguration.isCustom(),
                        metricsDescription.getParameters(),
                        metricsConfiguration.getOutputVersion()
                );
            }
        }

        LOG.error("MetricsDescription not found for metricType: {}", metricType);
        return null;
    }

    public Metric getOrCreateMetric(
            MetricType metricType,
            String callType,
            HashSet<AggregationParameter> aggregationParameters,
            boolean isCustom,
            Map<String, String> metricParameters,
            int outputVersion) {
        MetricsKey key = new MetricsKey(callType, metricType, aggregationParameters, isCustom);
        Metric metric = callMetrics.get(key);

        if (metric == null) {
            metric = createMetric(
                    metricType,
                    callType,
                    aggregationParameters,
                    metricParameters,
                    outputVersion
            );
            metric.resetUpdatedTime();
            callMetrics.put(key, metric);
        }

        return metric;
    }

    public String getMetrics() {
        StringBuilder result = new StringBuilder();
        ConcurrentHashMap<MetricsKey, Metric> metricsToDelete = new ConcurrentHashMap<MetricsKey, Metric>();
        for (Map.Entry<MetricsKey, Metric> metric : callMetrics.entrySet()) {
            if (!metric.getKey().isCustom()) {
                if ((System.currentTimeMillis() - metric.getValue().getUpdatedTime()) <= INTERVAL_TO_SHOW_METRIC) {
                    metric.getValue().print(result);
                } else {
                    metricsToDelete.put(metric.getKey(), metric.getValue());
                }
            }
        }

        for (MetricsKey metricToDelete : metricsToDelete.keySet()) {
            callMetrics.remove(metricToDelete);
        }

        for(SystemMetric metric : systemMetrics) {
            metric.print(result);
        }

        return result.toString();
    }

    public void resetMetrics() {
        callMetrics.clear();
        systemMetrics.clear();
    }

    private Metric createMetric(
            MetricType metricType,
            String callType,
            HashSet<AggregationParameter> aggregationParameters,
            Map<String, String> metricParameters,
            int outputVersion) {
        switch (metricType) {
            case COUNT:
                return new CountMetric(callType, aggregationParameters, outputVersion);
            case DURATION:
                return new DurationMetric(callType, aggregationParameters, metricParameters, outputVersion);
            case CPU:
                return new CPUMetric(callType, aggregationParameters, metricParameters, outputVersion);
            case QUEUE_WAIT_TIME:
                return new QueueWaitTimeMetric(callType, aggregationParameters, metricParameters, outputVersion);
            case TRANSACTIONS:
                return new TransactionMetric(callType, aggregationParameters, metricParameters, outputVersion);
            case DISK_IO:
                return new DiskIOMetric(callType, aggregationParameters, metricParameters, outputVersion);
            case NETWORK_IO:
                return new NetworkIOMetric(callType, aggregationParameters, metricParameters, outputVersion);
            case MEMORY:
                return new MemoryMetric(callType, aggregationParameters, metricParameters, outputVersion);
        }

        throw new RuntimeException("Incorrect metric name");
    }

    public void createSystemMetrics(List<MetricsDescription> metricsDescriptions) {
        for(MetricsDescription md : metricsDescriptions) {
            MetricType metricType = MetricType.getByConfigName(md.getName());
            if(metricType == null) throw new RuntimeException("Incorrect metric name");

            String metricName = md.getParameters().get("name");
            metricName = StringUtils.isEmpty(metricName) ? md.getName() : metricName;

            switch (metricType) {
                case PROFILER_DIRTY_BUFFERS:
                    systemMetrics.add(new DirtyBuffersMetric(metricName)); break;
                case PROFILER_EMPTY_BUFFERS:
                    systemMetrics.add(new EmptyBuffersMetric(metricName)); break;
                default:
                    throw new RuntimeException("Incorrect metric name");
            }
        }
    }
}
