package com.netcracker.profiler.security.csrf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class CsrfTokenServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        HttpSession session = request.getSession();
        try (OutputStream os = response.getOutputStream();) {
            os.write(
                    ("{\"header\":\"" + CSRFGuardHelper.CSRF_TOKEN_P + "\"" +
                            ",\"token\":\"" + CSRFGuardHelper.getToken(session) + "\"}").getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
