package com.netcracker.profiler.servlet;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.guice.DumpRootLocation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Retrieves raw stream from the
 */
@Singleton
public class RawData extends HttpServlet {
    private final File dumpRoot;

    @Inject
    public RawData(@DumpRootLocation File dumpRoot) {
        this.dumpRoot = dumpRoot;
    }

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

            File root = dumpRoot;
            File dirFile = new File(root, dir);
            File dirTypeFile = new File(dirFile, type);
            if(!dirTypeFile.toPath().normalize().startsWith(root.toPath())) {
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
