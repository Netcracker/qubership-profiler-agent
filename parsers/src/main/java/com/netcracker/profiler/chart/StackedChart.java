package com.netcracker.profiler.chart;

import com.netcracker.profiler.io.JSHelper;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class StackedChart {
    private final String title;
    private Map<String, TimeSeries> labels = new HashMap<String, TimeSeries>();

    public StackedChart(String title) {
        this.title = title;
    }

    public static class TimeSeries {
        int pos;
        long[] dates;
        int[] values;
        long total;
        long prevDate;

        public TimeSeries() {
            this(8);
        }

        public TimeSeries(int size) {
            dates = new long[size];
            values = new int[size];
        }

        public void add(long time, int value) {
            int pos = this.pos;
            ensureCapacity(pos);
            if (pos > 0 && time - prevDate > 1100) {
                dates[pos] = prevDate + 1000;
                values[pos] = 0;
                pos++;
                ensureCapacity(pos);
            }
            prevDate = time;
            dates[pos] = time;
            values[pos] = value;
            total += value;
            this.pos = pos + 1;
        }

        private void ensureCapacity(int pos) {
            if (dates.length > pos) {
                return;
            }
            long[] dates = new long[this.dates.length * 2];
            int[] values = new int[dates.length];
            System.arraycopy(this.dates, 0, dates, 0, this.dates.length);
            System.arraycopy(this.values, 0, values, 0, this.values.length);
            this.dates = dates;
            this.values = values;
        }

        public void end() {
            int pos = this.pos;
            if (pos == 0)
                return;
            add(dates[pos-1] + 1000, 0);
        }
    }

    public void add(long time, String label, int cnt) {
        TimeSeries series = getList(label);
        series.add(time, cnt);
    }

    private TimeSeries getList(String label) {
        TimeSeries series = labels.get(label);
        if (series == null)
            labels.put(label, series = new TimeSeries());
        return series;
    }

    public boolean isEmpty() {
        return labels.isEmpty();
    }

    public void toJS(Writer w) throws IOException {
        toJS(w, null);
    }

    public void toJS(Writer w, UnaryFunction<String, String> labelMapper) throws IOException {
        Map.Entry<String, TimeSeries>[] lines = labels.entrySet().toArray(new Map.Entry[labels.size()]);
        Arrays.sort(lines, new Comparator<Map.Entry<String, TimeSeries>>() {
            public int compare(Map.Entry<String, TimeSeries> a, Map.Entry<String, TimeSeries> b) {
                long totalA = a.getValue().total;
                long totalB = b.getValue().total;
                if (totalA != totalB)
                    return totalA > totalB ? 1 : -1;
                return a.getKey().compareTo(b.getKey());
            }
        });
        w.append("{labels:[");
        int maxLines = lines.length;
        if (maxLines > 0) {
            w.append("\"Date\"");
        }
        for (Map.Entry<String, TimeSeries> line : lines) {
            w.append(',');
            w.append('"');
            String label = line.getKey();
            if (labelMapper != null)
                label = labelMapper.evaluate(label);
            else
                label = JSHelper.escapeHTML(label);
            JSHelper.escapeJS(w, label);
            w.append('"');
        }
        w.append("],\ntitle:\"");
        JSHelper.escapeJS(w, JSHelper.escapeHTML(title));
        w.append("\",\ndata:[");

        TimeSeries[] series = new TimeSeries[maxLines];
        for (int i = 0; i < lines.length; i++) {
            series[i] = lines[i].getValue();
            series[i].end();
        }
        int[] it = new int[maxLines];
        boolean commaRequired = false;
        while (true) {
            long now = Long.MAX_VALUE;
            // Detect next minimal date
            for (int i = 0; i < maxLines; i++) {
                TimeSeries line = series[i];
                if (it[i] < line.pos && line.dates[it[i]] < now)
                    now = line.dates[it[i]];
            }
            if (now == Long.MAX_VALUE)
                break;
            if (commaRequired)
                w.append("\n,");
            else
                commaRequired = true;
            // Advance relevant series and emit javascript
            w.append("[new Date(");
            w.append(Long.toString(now));
            w.append(')');
            for (int i = 0; i < maxLines; i++) {
                TimeSeries line = series[i];
                w.append(',');
                if (it[i] < line.pos && line.dates[it[i]] == now) {
                    int val = line.values[it[i]];
                    w.append(Integer.toString(val));
                    it[i]++;
                } else
                    w.append("null");
            }
            w.append(']');
        }
        w.append("]}");
    }
}
