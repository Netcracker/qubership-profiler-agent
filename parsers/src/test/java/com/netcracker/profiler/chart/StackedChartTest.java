package com.netcracker.profiler.chart;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

public class StackedChartTest {
    @Test
    public void emptyChart() throws IOException {
        StackedChart c = new StackedChart("empty");
        StringWriter sw = new StringWriter();
        c.toJS(sw);
        assertEquals("{labels:[],\n" +
                "title:\"empty\",\n" +
                "data:[]}", sw.toString());
    }

    @Test
    public void singleCell() throws IOException {
        StackedChart c = new StackedChart("test ' title");
        c.add(10, "cpu", 15);

        StringWriter sw = new StringWriter();
        c.toJS(sw);
        assertEquals("{labels:[\"Date\",\"cpu\"],\n" +
                "title:\"test ' title\",\n" +
                "data:[[new Date(10),15]\n" +
                ",[new Date(1010),0]]}", sw.toString());
    }

    @Test
    public void singleHTMLTitle() throws IOException {
        StackedChart c = new StackedChart("test ' <a href>title");
        c.add(10, "cpu", 15);

        StringWriter sw = new StringWriter();
        c.toJS(sw);
        assertEquals("{labels:[\"Date\",\"cpu\"],\n" +
                "title:\"test ' &lt;a href&gt;title\",\n" +
                "data:[[new Date(10),15]\n" +
                ",[new Date(1010),0]]}", sw.toString());
    }

    @Test
    public void singleLabelMapper() throws IOException {
        StackedChart c = new StackedChart("test ' title");
        c.add(10, "cpu", 15);

        StringWriter sw = new StringWriter();
        c.toJS(sw, new UnaryFunction<String, String>() {
            public String evaluate(String arg) {
                return "<a href='example.com'>"+arg+"</a>";
            }
        });
        assertEquals("{labels:[\"Date\",\"<a href='example.com'>cpu<\\/a>\"],\n" +
                "title:\"test ' title\",\n" +
                "data:[[new Date(10),15]\n" +
                ",[new Date(1010),0]]}", sw.toString());
    }

    @Test
    public void simpleSeries() throws IOException {
        StackedChart c = new StackedChart("test \" title");
        c.add(10, "cpu", 5);
        c.add(20, "cpu", 6);
        c.add(30, "cpu", 1);

        StringWriter sw = new StringWriter();
        c.toJS(sw);

        assertEquals("{labels:[\"Date\",\"cpu\"],\n" +
                "title:\"test \\\" title\",\n" +
                "data:[[new Date(10),5]\n" +
                ",[new Date(20),6]\n" +
                ",[new Date(30),1]\n" +
                ",[new Date(1030),0]]}", sw.toString());
    }

    @Test
    public void twoSimpleSeries() throws IOException {
        StackedChart c = new StackedChart("test title");
        c.add(15, "io", 1);
        c.add(10, "cpu", 5);
        c.add(20, "cpu", 6);
        c.add(20, "io", 2);
        c.add(25, "io", 1);
        c.add(27, "io", 2);
        c.add(30, "cpu", 1);
        c.add(31, "io", 2);

        StringWriter sw = new StringWriter();
        c.toJS(sw);

        assertEquals("{labels:[\"Date\",\"io\",\"cpu\"],\n" +
                "title:\"test title\",\n" +
                "data:[[new Date(10),null,5]\n" +
                ",[new Date(15),1,null]\n" +
                ",[new Date(20),2,6]\n" +
                ",[new Date(25),1,null]\n" +
                ",[new Date(27),2,null]\n" +
                ",[new Date(30),null,1]\n" +
                ",[new Date(31),2,null]\n" +
                ",[new Date(1030),null,0]\n" +
                ",[new Date(1031),0,null]]}", sw.toString());
    }

    @Test
    public void twoSimpleSeriesOrder() throws IOException {
        StackedChart c = new StackedChart("test title");
        c.add(15, "io", 1);
        c.add(10, "cpu", 5);
        c.add(20, "cpu", 6);
        c.add(20, "io", 2);
        c.add(25, "io", 1);
        c.add(27, "io", 2);
        c.add(30, "cpu", 1);
        c.add(31, "io", 9);

        StringWriter sw = new StringWriter();
        c.toJS(sw);

        assertEquals("{labels:[\"Date\",\"cpu\",\"io\"],\n" +
                "title:\"test title\",\n" +
                "data:[[new Date(10),5,null]\n" +
                ",[new Date(15),null,1]\n" +
                ",[new Date(20),6,2]\n" +
                ",[new Date(25),null,1]\n" +
                ",[new Date(27),null,2]\n" +
                ",[new Date(30),1,null]\n" +
                ",[new Date(31),null,9]\n" +
                ",[new Date(1030),0,null]\n" +
                ",[new Date(1031),null,0]]}", sw.toString());
    }

    @Test
    public void finishingAfter1sec() throws IOException {
        StackedChart c = new StackedChart("test title");
        c.add(15000, "io", 1);
        c.add(10000, "cpu", 5);
        c.add(20000, "cpu", 6);
        c.add(20000, "io", 2);
        c.add(25000, "io", 1);
        c.add(26000, "io", 2);
        c.add(30000, "cpu", 1);
        c.add(31000, "io", 9);

        StringWriter sw = new StringWriter();
        c.toJS(sw);

        assertEquals("{labels:[\"Date\",\"cpu\",\"io\"],\n" +
                "title:\"test title\",\n" +
                "data:[[new Date(10000),5,null]\n" +
                ",[new Date(11000),0,null]\n" +
                ",[new Date(15000),null,1]\n" +
                ",[new Date(16000),null,0]\n" +
                ",[new Date(20000),6,2]\n" +
                ",[new Date(21000),0,0]\n" +
                ",[new Date(25000),null,1]\n" +
                ",[new Date(26000),null,2]\n" +
                ",[new Date(27000),null,0]\n" +
                ",[new Date(30000),1,null]\n" +
                ",[new Date(31000),0,9]\n" +
                ",[new Date(32000),null,0]]}", sw.toString());
    }
}
