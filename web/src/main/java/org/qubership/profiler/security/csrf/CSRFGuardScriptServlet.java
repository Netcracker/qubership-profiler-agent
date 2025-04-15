package org.qubership.profiler.security.csrf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class CSRFGuardScriptServlet extends HttpServlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/x-javascript; charset=utf-8");
        HttpSession session = request.getSession();
        final PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "utf-8")), false);
        out.print(getCSRFScript(session));
        out.flush();
    }

    private String getCSRFScript(HttpSession session) {
        StringBuilder sb = new StringBuilder();
        sb.append("var CSRF_TOKEN_NAME='")
                .append(CSRFGuardHelper.CSRF_TOKEN_P)
                .append("';\n")
                .append("var CSRF_TOKEN_VALUE='")
                .append(CSRFGuardHelper.getToken(session))
                .append("';\n")
                .append("function csrfSafeMethod(method) {\n" +
                        "\t// these HTTP methods do not require CSRF protection\n" +
                        "\treturn (/^(GET)$/.test(method));\n" +
                        "}\n" +
                        "\n" +
                        "$.ajaxSetup({\n" +
                        "\tbeforeSend: function(xhr, settings) {\n" +
                        "\t\tif (!csrfSafeMethod(settings.type) && !this.crossDomain) {\n" +
                        "\t\t\txhr.setRequestHeader(CSRF_TOKEN_NAME, CSRF_TOKEN_VALUE);\n" +
                        "\t\t}\n" +
                        "\t}\n" +
                        "});");
        return sb.toString();
    }

}
