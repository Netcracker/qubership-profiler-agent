package org.qubership.profiler.servlet;

import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.dump.DumpRootResolver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Retrieves raw stream from the
 */
public class RawData extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String dir = request.getParameter("dir");
        String type = request.getParameter("type");
        int file = Integer.parseInt(request.getParameter("file"));
        int offs = Integer.parseInt(request.getParameter("offs"));

        ZipOutputStream zip = null;

        try {
            response.setBufferSize(65536);
            response.setContentType("application/octet-stream");
            zip = new ZipOutputStream(response.getOutputStream());

            final String zipName = request.getPathInfo();
            zip.putNextEntry(new ZipEntry(zipName.substring(1, zipName.length() - 4)));

            File root = new File(DumpRootResolver.dumpRoot).getParentFile();
            File dirFile = new File(root, dir);
            File dirTypeFile = new File(dirFile, type);
            if(!dirTypeFile.getCanonicalPath().startsWith(root.getCanonicalPath())) {
                throw new IllegalArgumentException("Access denied. The path is outside of dump folder.");
            }

            DataInputStreamEx dis = DataInputStreamEx.openDataInputStream(dirFile, type, file);
            dis.skip(offs);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
            dis.readString(bw, Integer.MAX_VALUE);
            bw.flush();
            zip.closeEntry();
        } finally {
            if (zip != null)
                zip.close();
        }
    }
}
