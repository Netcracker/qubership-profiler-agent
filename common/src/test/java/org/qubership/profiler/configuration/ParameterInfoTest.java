package org.qubership.profiler.configuration;

import org.qubership.profiler.agent.ParamTypes;
import org.qubership.profiler.agent.ParameterInfo;

import org.testng.Assert;
import org.testng.annotations.Test;
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

        Assert.assertTrue(pi.big);
        Assert.assertFalse(pi.deduplicate);
        Assert.assertEquals(pi.combined, ParamTypes.PARAM_BIG);
    }

    @Test
    public void sqlTypeAutoParse() {
        ParameterInfo pi = getPI("<param-info name=\"abcd.sql\"/>");

        Assert.assertTrue(pi.big);
        Assert.assertTrue(pi.deduplicate);
        Assert.assertEquals(pi.combined, ParamTypes.PARAM_BIG_DEDUP);
    }

    @Test
    public void xpathTypeAutoParse() {
        ParameterInfo pi = getPI("<param-info name=\"xpath.abcd\"/>");

        Assert.assertTrue(pi.big);
        Assert.assertTrue(pi.deduplicate);
        Assert.assertEquals(pi.combined, ParamTypes.PARAM_BIG_DEDUP);
    }

    @Test
    public void attrName() {
        ParameterInfo pi = getPI("<param-info name=\"test\"/>");

        Assert.assertEquals(pi.name, "test");
    }

    @Test
    public void attrBig() {
        ParameterInfo pi = getPI("<param-info name=\"test\" big=\"true\"/>");

        Assert.assertTrue(pi.big);
        Assert.assertFalse(pi.deduplicate);
        Assert.assertEquals(pi.combined, ParamTypes.PARAM_BIG);
    }

    @Test
    public void attrDeduplicate() {
        ParameterInfo pi = getPI("<param-info name=\"test\" deduplicate=\"true\"/>");

        Assert.assertTrue(pi.big);
        Assert.assertTrue(pi.deduplicate);
        Assert.assertEquals(pi.combined, ParamTypes.PARAM_BIG_DEDUP);
    }

    @Test
    public void attrBigDeduplicate() {
        ParameterInfo pi = getPI("<param-info name=\"test\" big=\"true\" deduplicate=\"true\"/>");

        Assert.assertTrue(pi.big);
        Assert.assertTrue(pi.deduplicate);
        Assert.assertEquals(pi.combined, ParamTypes.PARAM_BIG_DEDUP);
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
