package com.netcracker.profiler.security.csrf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

@Singleton
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
