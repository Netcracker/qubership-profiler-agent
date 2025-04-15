package org.qubership.profiler.metrics;

import org.qubership.profiler.agent.HistogramIterationType;
import org.qubership.profiler.agent.MetricType;

import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.SynchronizedHistogram;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractHistogramMetric extends AbstractMetric {
    public static final String BUCKET_SUFFIX = "bucket";
    public static final String SUM_SUFFIX = "sum";
    public static final String COUNT_SUFFIX = "count";
    protected SynchronizedHistogram histogram;
    protected AtomicLong sum = new AtomicLong();
    protected long lowestDiscernibleValue=1;
    protected long highestTrackableValue=10000000;
    protected int numberOfSignificantValueDigits=2;
    protected HistogramIterationType histogramIterationType = HistogramIterationType.LOGARITHMIC_BUCKET_VALUES;
    protected int percentileTicksPerHalfDistance=4;
    protected long valueUnitsInFirstBucket=100;
    protected double logBase=2;
    protected long valueUnitsPerBucket=100;

    private String sumKey;
    private String countKey;

    public AbstractHistogramMetric(String callType, MetricType type, HashSet<AggregationParameter> aggregationParameters, MetricUnit metricUnit,
                                   int outputVersion, String suffix) {
        super(callType, type, aggregationParameters, metricUnit, outputVersion, suffix);
        sumKey = buildKey(callType, type, aggregationParameters, metricUnit, SUM_SUFFIX);
        countKey = buildKey(callType, type, aggregationParameters, MetricUnit.TOTAL, COUNT_SUFFIX);
    }

    public void resetValue() {
        histogram.reset();
        sum.set(0);
    }

    protected void initHistogram() {
        histogram = new SynchronizedHistogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
    }

    protected void parseHistogramParameters(Map<String, String> metricParameters) {
        String lowestDiscernibleValueStr = metricParameters.get("lowestDiscernibleValue");
        if(!StringUtils.isEmpty(lowestDiscernibleValueStr)) {
            lowestDiscernibleValue = Long.parseLong(lowestDiscernibleValueStr);
        }
        String highestTrackableValueStr = metricParameters.get("highestTrackableValue");
        if(!StringUtils.isEmpty(highestTrackableValueStr)) {
            highestTrackableValue = Long.parseLong(highestTrackableValueStr);
        }
        String numberOfSignificantValueDigitsStr = metricParameters.get("numberOfSignificantValueDigits");
        if(!StringUtils.isEmpty(numberOfSignificantValueDigitsStr)) {
            numberOfSignificantValueDigits = Integer.parseInt(numberOfSignificantValueDigitsStr);
        }

        HistogramIterationType iterationType = HistogramIterationType.getByConfigName(metricParameters.get("iterationType"));
        if(iterationType != null) {
            histogramIterationType = iterationType;
        }

        String valueUnitsInFirstBucketStr = metricParameters.get("valueUnitsInFirstBucket");
        if(!StringUtils.isEmpty(valueUnitsInFirstBucketStr)) {
            valueUnitsInFirstBucket = Long.parseLong(valueUnitsInFirstBucketStr);
        }
        String logBaseStr = metricParameters.get("logBase");
        if(!StringUtils.isEmpty(logBaseStr)) {
            logBase = Double.parseDouble(logBaseStr);
        }
        String valueUnitsPerBucketStr = metricParameters.get("valueUnitsPerBucket");
        if(!StringUtils.isEmpty(valueUnitsPerBucketStr)) {
            valueUnitsPerBucket = Long.parseLong(valueUnitsPerBucketStr);
        }
        String percentileTicksPerHalfDistanceStr = metricParameters.get("percentileTicksPerHalfDistance");
        if(!StringUtils.isEmpty(percentileTicksPerHalfDistanceStr)) {
            percentileTicksPerHalfDistance = Integer.parseInt(percentileTicksPerHalfDistanceStr);
        }
    }

    protected void recordValue(long value) {
        if(value>highestTrackableValue) {
            value = highestTrackableValue;
        }
        histogram.recordValue(value);
        sum.addAndGet(value);
    }

    private Iterable<HistogramIterationValue> getHistogramIterator() {
        switch (histogramIterationType) {
            case PERCENTILES:
                return histogram.percentiles(percentileTicksPerHalfDistance);
            case LINEAR_BUCKET_VALUES:
                return histogram.linearBucketValues(valueUnitsPerBucket);
            case LOGARITHMIC_BUCKET_VALUES:
                return histogram.logarithmicBucketValues(valueUnitsInFirstBucket, logBase);
            case ALL_VALUES:
                return histogram.allValues();
            case RECORDED_VALUES:
                return histogram.recordedValues();
        }
        return null;
    }

    public void print(StringBuilder out) {
        for (HistogramIterationValue histogramIterationValue : getHistogramIterator()) {
            if (histogramIterationValue.getTotalCountToThisValue() != 0) {
                out.append(key);
                if(out.charAt(out.length()-1) != '{') {
                    out.append(", ");
                }
                out.append("le=\"").
                        append(histogramIterationValue.getValueIteratedTo()).
                        append("\"} ").
                        append(histogramIterationValue.getTotalCountToThisValue()).
                        append("\n");
            }
        }

        out.append(key);
        if(out.charAt(out.length()-1) != '{') {
            out.append(", ");
        }
        out.append("le=\"+Inf\"} ").
                append(histogram.getTotalCount()).
                append("\n");

        out.append(sumKey)
                .append("} ")
                .append(sum.get())
                .append("\n");

        out.append(countKey)
                .append("} ")
                .append(histogram.getTotalCount())
                .append("\n");
    }
}
