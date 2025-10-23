package com.netcracker.cdt.ui.services.tree;

import java.io.*;

public class SinglePageLayout implements AutoCloseable {
    public static final String JAVASCRIPT = "single-page-javascript";
    public static final String HTML = "single-page-html";
    public static final String CSS_START = "<link type=\"text/css\" href=\"";

    private final Template template;
    protected boolean isWritingHTML;

    public static void append(String fileName, OutputStream out) throws IOException {
        final InputStream is = SinglePageLayout.class.getResourceAsStream(fileName);
        if (is == null) return;
        int read;
        byte[] buf = new byte[4096];
        try {
            while ((read = is.read(buf)) > 0)
                out.write(buf, 0, read);
        } finally {
            is.close();
        }
    }

    public static Template getTemplate(String path, String charsetName) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        append(path, baos);
        String html = baos.toString(charsetName);
        return new Template(html, charsetName);
    }

    public SinglePageLayout(Template template) {
        this.template = template;
    }

    protected void printPageStart() throws IOException {
        isWritingHTML = true;
//        template.appendStart(super.getOutputStream());
    }

    protected void maybeFinishPage() throws IOException {
        if (!isWritingHTML)
            return;
        isWritingHTML = false;
//        template.appendEnd(super.getOutputStream());
    }

    public void putNextEntry(String id, String name, String type) throws IOException {
        if (JAVASCRIPT.equals(id)) {
//            super.putNextEntry(HTML, name, "text/html");
            printPageStart();
            return;
        }
        maybeFinishPage();
//        super.putNextEntry(id, name, type);
    }

    @Override
    public void close() throws IOException {
        maybeFinishPage();
    }

    public static class Template {
        private final static byte[] OPEN_CSS = "<style type='text/css'>".getBytes();
        private final static byte[] CLOSE_CSS = "</style>".getBytes();

        public final byte[] headOpenCss;
        public final byte[] closeCssOpenJs;
        public final byte[] closeJs;

        private final String cssFile;
        private final String jsFile;

        public Template(String html, String charsetName) {

            int cssStart = html.indexOf(CSS_START);
            int startIndex = cssStart + CSS_START.length();
            int endIndex = html.indexOf('"', cssStart + CSS_START.length());
            cssFile = "/" + html.substring(startIndex, endIndex);
            int cssEnd = html.indexOf("/>", cssStart + CSS_START.length()) + 2;

            int jsStart = html.indexOf("(function(){");
            int jsFileStart = html.indexOf("src=\"js/", jsStart) + 5;
            final int jsFileEnd = html.indexOf('"', jsFileStart);
            jsFile = "/" + html.substring(jsFileStart, jsFileEnd);

            try {
                headOpenCss = html.substring(0, cssStart).getBytes(charsetName);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Unsupported charset " + charsetName, e);
            }

            try {
                closeCssOpenJs = html.substring(cssEnd, jsStart).getBytes(charsetName);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Unsupported charset " + charsetName, e);
            }

            try {
                closeJs = html.substring(jsFileEnd + 2).getBytes(charsetName);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Unsupported charset " + charsetName, e);
            }
        }

        public void appendStart(OutputStream out) throws IOException {
            out.write(headOpenCss);
            out.write(OPEN_CSS);
//            resources.append(cssFile, out);
            out.write(CLOSE_CSS);
            out.write(closeCssOpenJs);
//            resources.append(jsFile, out);

        }

        public void appendEnd(OutputStream out) throws IOException {
            out.write(closeJs);
        }
    }
}
