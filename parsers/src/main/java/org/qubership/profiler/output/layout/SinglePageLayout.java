package org.qubership.profiler.output.layout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

public class SinglePageLayout extends LayoutDelegate {
    public static final String JAVASCRIPT = "single-page-javascript";
    public static final String HTML = "single-page-html";

    private final Template template;

    protected boolean isWritingHTML;

    private static final Logger LOGGER = LoggerFactory.getLogger(SinglePageLayout.class);

    public static Template getTemplate(String html, Charset charset) throws IOException {
        return new Template(html, charset);
    }

    public SinglePageLayout(Layout parent, Template template) {
        super(parent);
        this.template = template;
    }

    protected void printPageStart() throws IOException {
        isWritingHTML = true;
        template.appendStart(super.getOutputStream());
    }

    protected void maybeFinishPage() throws IOException {
        if (!isWritingHTML)
            return;
        isWritingHTML = false;
        template.appendEnd(super.getOutputStream());
    }

    @Override
    public void putNextEntry(String id, String name, String type) throws IOException {
        if (JAVASCRIPT.equals(id)) {
            super.putNextEntry(HTML, name, "text/html");
            printPageStart();
            return;
        }
        maybeFinishPage();

        super.putNextEntry(id, name, type);
    }

    @Override
    public void close() throws IOException {
        maybeFinishPage();
        super.close();
    }

    public static class Template {
        public final byte[] openJs;
        public final byte[] closeJs;

        public Template(String html, Charset charset) {
            // The beginning of the file includes all the CSS/JS dependencies, so we search backwards
            int jsEnd = html.lastIndexOf("</script>");

            openJs = html.substring(0, jsEnd).getBytes(charset);
            closeJs = html.substring(jsEnd).getBytes(charset);
        }

        public void appendStart(OutputStream out) throws IOException {
            out.write(openJs);
        }

        public void appendEnd(OutputStream out) throws IOException {
            out.write(closeJs);
        }
    }
}
