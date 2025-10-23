package com.netcracker.common.search.filter;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class FilterParserTest {

    @Test
    void testEmpty() {
        var p = FilterParser.parse("");
        assertThat(p.mandatory(), empty());
        assertThat(p.included(), empty());
        assertThat(p.excluded(), empty());
    }

    @Test
    void testOneWord() {
        var p = FilterParser.parse("asd");
        assertThat(p.mandatory(), empty());
        assertThat(p.included(), hasStringValues("asd"));
        assertThat(p.excluded(), empty());
    }

    @Test
    void testWords() {
        var p = FilterParser.parse("worker complex.word param=value +CAPS +\"quoted\" +$param -321");
        assertThat(p.mandatory(), hasStringValues("caps", "quoted", "$param"));
        assertThat(p.included(), hasStringValues("worker", "complex.word", "param=value"));
        assertThat(p.excluded(), hasStringValues("321"));
    }

    @Test
    void testQuotes() {
        var p = FilterParser.parse("worker +\"quoted\" ");
        assertThat(p.mandatory(), hasStringValues("quoted"));
        assertThat(p.included(), hasStringValues("worker"));

        p = FilterParser.parse("worker +\"quoted phrase\" ");
        assertThat(p.mandatory(), hasStringValues("quoted phrase"));
        assertThat(p.included(), hasStringValues("worker"));

        p = FilterParser.parse("worker +'quoted' ");
        assertThat(p.mandatory(), hasStringValues("quoted"));
        assertThat(p.included(), hasStringValues("worker"));

        p = FilterParser.parse("worker +'quoted phrase' ");
        assertThat(p.mandatory(), hasStringValues("quoted phrase"));
        assertThat(p.included(), hasStringValues("worker"));

        p = FilterParser.parse("worker +`quoted` ");
        assertThat(p.mandatory(), hasStringValues("quoted"));
        assertThat(p.included(), hasStringValues("worker"));

        p = FilterParser.parse("worker +`quoted phrase` ");
        assertThat(p.mandatory(), hasStringValues("quoted phrase"));
        assertThat(p.included(), hasStringValues("worker"));
    }

    @Test
    void testParametersWords() {
        var p = FilterParser.parse("+$param.1=val -$param.2=val2 $param=value");
        assertThat(p.mandatory(), hasParamerValue("param.1", "val"));
        assertThat(p.included(), hasParamerValue("param", "value"));
        assertThat(p.excluded(), hasParamerValue("param.2", "val2"));
    }

    @Test
    void testQuotedParametersWords() {
        var p = FilterParser.parse("+$'param.1'='val' -$\"param.2\"=val2 $`param`=value");
        assertThat(p.mandatory(), hasParamerValue("param.1", "val"));
        assertThat(p.included(), hasParamerValue("param", "value"));
        assertThat(p.excluded(), hasParamerValue("param.2", "val2"));
    }

    void testComplexQuotedParameters() {
        // quotes don't work for parameters name/values
        var p = FilterParser.parse("$web.url=http://10.131.130.171:8180 -$profiler.title=`client: 192.168.206.37`");
        assertThat(p.mandatory(), hasSize(0));
        assertThat(p.included(), hasParamerValue("web.url", "http://10.131.130.171:8180"));
        assertThat(p.excluded(), hasParamerValue("profiler.title", "client: 192.168.206.37"));
    }

    @Test
    void testParameterMatcher() {
        var p = List.of(FilterValue.from("param.1", "val"));
        assertThat(p, hasParamerValue("param.1", "val"));
        assertThat(p, not(hasParamerValue("param.12", "val")));
        assertThat(p, not(hasParamerValue("param.1", "val2")));
    }

    private static Matcher<List<FilterValue>> hasParamerValue(String name, String value) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(List<FilterValue> item) {
                if (item.size() != 1) {
                    return false;
                }
                return FilterValue.from(name, value).equals(item.get(0));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be " + FilterValue.from(name, value));
            }
        };
    }

    private static Matcher<List<FilterValue>> hasStringValues(String... vals) { // strict comparison
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(List<FilterValue> item) {
                if (item.size() != vals.length) {
                    return false;
                }
                for (int i = 0; i < vals.length; i++) {
                    if (!vals[i].equals(item.get(i).value())) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be ").appendValueList("[", ",", "]", vals);
            }
        };
    }
}