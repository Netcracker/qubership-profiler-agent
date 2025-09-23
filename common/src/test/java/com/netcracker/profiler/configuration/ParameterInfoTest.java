package com.netcracker.profiler.configuration;

import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.agent.ParamTypes;
import com.netcracker.profiler.agent.ParameterInfo;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class ParameterInfoTest {
    @Test
    public void xmlTypeAutoParse() {
        ParameterInfo pi = getPI("<param-info name=\"abcd.xml\"/>");

        assertTrue(pi.big);
        assertFalse(pi.deduplicate);
        assertEquals(ParamTypes.PARAM_BIG, pi.combined);
    }

    @Test
    public void sqlTypeAutoParse() {
        ParameterInfo pi = getPI("<param-info name=\"abcd.sql\"/>");

        assertTrue(pi.big);
        assertTrue(pi.deduplicate);
        assertEquals(ParamTypes.PARAM_BIG_DEDUP, pi.combined);
    }

    @Test
    public void xpathTypeAutoParse() {
        ParameterInfo pi = getPI("<param-info name=\"xpath.abcd\"/>");

        assertTrue(pi.big);
        assertTrue(pi.deduplicate);
        assertEquals(ParamTypes.PARAM_BIG_DEDUP, pi.combined);
    }

    @Test
    public void attrName() {
        ParameterInfo pi = getPI("<param-info name=\"test\"/>");

        assertEquals("test", pi.name);
    }

    @Test
    public void attrBig() {
        ParameterInfo pi = getPI("<param-info name=\"test\" big=\"true\"/>");

        assertTrue(pi.big);
        assertFalse(pi.deduplicate);
        assertEquals(pi.combined, ParamTypes.PARAM_BIG);
    }

    @Test
    public void attrDeduplicate() {
        ParameterInfo pi = getPI("<param-info name=\"test\" deduplicate=\"true\"/>");

        assertTrue(pi.big);
        assertTrue(pi.deduplicate);
        assertEquals(ParamTypes.PARAM_BIG_DEDUP, pi.combined);
    }

    @Test
    public void attrBigDeduplicate() {
        ParameterInfo pi = getPI("<param-info name=\"test\" big=\"true\" deduplicate=\"true\"/>");

        assertTrue(pi.big);
        assertTrue(pi.deduplicate);
        assertEquals(ParamTypes.PARAM_BIG_DEDUP, pi.combined);
    }

    private ParameterInfo getPI(String xml) {
        Element e = parseXML(xml);
        ParameterInfo pi = new ParameterInfo(e);
        pi.parse(e);
        return pi;
    }

    private Element parseXML(String xml) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new InputSource(new StringReader(xml)));
            return doc.getDocumentElement();
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unable to parse xml " + xml, e);
        }
    }
}
