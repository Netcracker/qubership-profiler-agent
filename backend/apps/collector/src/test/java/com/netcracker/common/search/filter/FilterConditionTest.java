package com.netcracker.common.search.filter;

import com.netcracker.common.models.dict.ParameterValue;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

class FilterConditionTest {

    @Test
    void matcherReportsIntermediateFailure() {
        var c = FilterParser.parse("worker +complex -ignored");

        var t = c.start(true);
        assertThat(t.addGeneralString("worker"), equalTo(false));
        assertThat(t.addGeneralString("worker2"), equalTo(false));
        assertThat(t.addGeneralString("worke"), equalTo(false));
        assertThat(t.addGeneralString("complex"), equalTo(false));
        assertThat(t.addGeneralString("complex2"), equalTo(false));
        assertThat(t.addGeneralString("comple"), equalTo(false));
        assertThat(t.addGeneralString("ignored"), equalTo(true));
        assertThat(t.addGeneralString("ignored2"), equalTo(true));
        assertThat(t.addGeneralString("ignore"), equalTo(false));
    }

    @Test
    void matchesGeneralStrings() {
        var c = FilterParser.parse("option worker +complex +mandatory -ignored");

        assertThat(filterMatchesGeneralStrings(c, "complex mandatory"), equalTo(false));
        assertThat(filterMatchesGeneralStrings(c, "worker complex mandatory"), equalTo(true));
        assertThat(filterMatchesGeneralStrings(c, "option complex mandatory"), equalTo(true));
        assertThat(filterMatchesGeneralStrings(c, "option complexed mandatory2"), equalTo(true));

        assertThat(filterMatchesGeneralStrings(c, "worker complex"), equalTo(false));
        assertThat(filterMatchesGeneralStrings(c, "worker mandatory"), equalTo(false));
        assertThat(filterMatchesGeneralStrings(c, "complex"), equalTo(false));
        assertThat(filterMatchesGeneralStrings(c, "mandatory"), equalTo(false));

        assertThat(filterMatchesGeneralStrings(c, "complex mandatory ignored"), equalTo(false));
        assertThat(filterMatchesGeneralStrings(c, "complex mandatory ignored.long"), equalTo(false));
        assertThat(filterMatchesGeneralStrings(c, "worker complex mandatory ignore"), equalTo(true));
        assertThat(filterMatchesGeneralStrings(c, "worker ignore"), equalTo(false));
    }

    private boolean filterMatchesGeneralStrings(FilterCondition c, String... lines) {
        var t = c.start(true);
        for (var s: lines) {
            t.addGeneralString(s);
        }
        return t.matches();
    }

    @Test
    void matchesParameterValuesWithNames() {
        var c = FilterParser.parse("+$param1=complex -$param2=ignored");

        assertThat(filterMatchesParameterValuesWithNames(c, param("param1", "complex mandatory")), equalTo(true));
        assertThat(filterMatchesParameterValuesWithNames(c, param("param2", "complex mandatory")), equalTo(false));
        assertThat(filterMatchesParameterValuesWithNames(c, param("param1", "comple mandatory")), equalTo(false));
        assertThat(filterMatchesParameterValuesWithNames(c, param("param1", "complex mandatory"), param("param3", "ignored")), equalTo(true));
        assertThat(filterMatchesParameterValuesWithNames(c, param("param1", "complex mandatory"), param("param2", "ignore")), equalTo(true));
        assertThat(filterMatchesParameterValuesWithNames(c, param("param1", "complex mandatory"), param("param2", "ignored")), equalTo(false));
    }

    private boolean filterMatchesParameterValuesWithNames(FilterCondition c, ParameterValue... lines) {
        var t = c.start(true);
        for (var s: lines) {
            t.addParameterValuesByName(s.name(), s.values());
        }
        return t.matches();
    }

    private ParameterValue param(String name, String... values) {
        return new ParameterValue(name, Arrays.asList(values));
    }


    @Test
    void matchesParameterValuesWithIds() {
        var c = FilterParser.parse("+$param1=complex -$param2=ignored");
        c = c.copyWithPopulatedIds(Map.of(
                "param1", 1,
                "param2", 2)
        );

        assertThat(filterMatchesParameterValuesWithIds(c, param(1, "complex mandatory")), equalTo(true));
        assertThat(filterMatchesParameterValuesWithIds(c, param(2, "complex mandatory")), equalTo(false));
        assertThat(filterMatchesParameterValuesWithIds(c, param(1, "comple mandatory")), equalTo(false));
        assertThat(filterMatchesParameterValuesWithIds(c, param(1, "complex mandatory"), param(3, "ignored")), equalTo(true));
        assertThat(filterMatchesParameterValuesWithIds(c, param(1, "complex mandatory"), param(2, "ignore")), equalTo(true));
        assertThat(filterMatchesParameterValuesWithIds(c, param(1, "complex mandatory"), param(2, "ignored")), equalTo(false));
    }

    private boolean filterMatchesParameterValuesWithIds(FilterCondition c, IdParameterValue... lines) {
        var t = c.start(true);
        for (var s: lines) {
            t.addParameterValuesById(s.id(), s.values());
        }
        return t.matches();
    }

    private IdParameterValue param(int id, String... values) {
        return new IdParameterValue(id, Arrays.asList(values));
    }
}

record IdParameterValue(int id, List<String> values) {}