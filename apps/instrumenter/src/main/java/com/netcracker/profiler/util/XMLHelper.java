package com.netcracker.profiler.util;

import org.w3c.dom.DOMException;
import org.w3c.dom.Node;

public class XMLHelper {
    public static String getTextContent(Node node) throws DOMException {
        String textContent = "";

        if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
            textContent = node.getNodeValue();
        } else {
            Node child = node.getFirstChild();
            if (child != null) {
                Node sibling = child.getNextSibling();
                if (sibling != null) {
                    StringBuffer sb = new StringBuffer();
                    getTextContent(node, sb);
                    textContent = sb.toString();
                } else {
                    if (child.getNodeType() == Node.TEXT_NODE) {
                        textContent = child.getNodeValue();
                    } else {
                        textContent = getTextContent(child);
                    }
                }
            }
        }

        return textContent;
    }


    private static void getTextContent(Node node, StringBuffer sb) throws DOMException {
        Node child = node.getFirstChild();
        while (child != null) {
            if (child.getNodeType() == Node.TEXT_NODE) {
                sb.append(child.getNodeValue());
            } else {
                getTextContent(child, sb);
            }
            child = child.getNextSibling();
        }
    }

}
