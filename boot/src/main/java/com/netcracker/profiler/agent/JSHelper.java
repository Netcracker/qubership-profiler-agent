package com.netcracker.profiler.agent;

import java.io.IOException;
import java.io.Writer;

public class JSHelper {
    final static char hex[] = "0123456789ABCDEF".toCharArray();

    public static void escapeJS(Writer out, String s) throws IOException {
        if (s != null)
            escapeJS(out, s, 0, s.length());
    }

    public static void escapeJS(Writer out, String s, int offs, int len) throws IOException {
        char[] hex = JSHelper.hex;
        if (s == null) return;
        len = Math.min(len, s.length());
        for (int i = offs; i < len; i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"':
                    out.write('\\');
                    out.write('\"');
                    break;
                case '\\':
                    out.write('\\');
                    out.write('\\');
                    break;
                case '\b':
                    out.write('\\');
                    out.write('b');
                    break;
                case '\f':
                    out.write('\\');
                    out.write('f');
                    break;
                case '\n':
                    out.write('\\');
                    out.write('n');
                    break;
                case '\r':
                    out.write('\\');
                    out.write('r');
                    break;
                case '\t':
                    out.write('\\');
                    out.write('t');
                    break;
                case '/':
                    out.write('\\');
                    out.write('/');
                    break;
                default:
                    //Reference: http://www.unicode.org/versions/Unicode5.1.0/
                    if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF')) {
                        out.write(hex[ch >>> 12]);
                        out.write(hex[(ch >>> 8) & 15]);
                        out.write(hex[(ch >>> 4) & 15]);
                        out.write(hex[(ch) & 15]);
                    } else
                        out.write(ch);
            }
        }
    }

    public static String escapeHTML(String src) {
        if (src == null) return null;
        return src.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    }

    public static boolean hasNLines(String s, int nlines) {
        if (s == null || s.length() == 0) return nlines <= 1;
        int pos = 0;
        for (; pos != -1 && nlines > 1; nlines--) {
            pos = s.indexOf('\n', pos + 1);
        }
        return nlines <= 1;
    }
}
