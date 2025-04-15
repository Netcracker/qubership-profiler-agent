package org.qubership.profiler.servlet;

import org.qubership.profiler.config.AnalyzerWhiteList;
import org.qubership.profiler.io.FileNameUtils;
import org.qubership.profiler.io.JSHelper;

import java.io.*;
import java.net.URLEncoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ThreadDump extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String pathInfo = request.getPathInfo();
        if ("/file_size".equals(pathInfo))
            getFileSize(request, response);
    }

    private void getFileSize(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String callback = request.getParameter("callback");
        if (callback == null) callback = "dataReceived";
        callback = URLEncoder.encode(callback, "UTF-8");

        response.setContentType("application/x-javascript; charset=utf-8");
        final PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "utf-8")), false);

        out.print(callback);
        out.print('(');
        out.print(URLEncoder.encode(request.getParameter("id"), "UTF-8"));
        out.print(", ['");

        String fileName = FileNameUtils.trimFileName(request.getParameter("file"));
        if (fileName == null || fileName.length() == 0) {
            String weblogicName = System.getProperty("weblogic.management.server") == null ? null : System.getProperty("weblogic.Name");
            if (weblogicName != null)
                fileName = "servers/" + weblogicName + "/logs/" + weblogicName + ".out";
            else
                fileName = "logs/console.log";
        }

        JSHelper.escapeJS(out, fileName);
        out.print("',");
        File file = new File(fileName);
        if(!AnalyzerWhiteList.checkAccess(file))
            out.print(-3);
        else if (!file.exists())
            out.print(-1);
        else if (file.isDirectory())
            out.print(-2);
        else
            out.print(file.length());
        out.print("]);");
        out.close();
    }
}
