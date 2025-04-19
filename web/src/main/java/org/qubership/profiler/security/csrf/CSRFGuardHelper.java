package org.qubership.profiler.security.csrf;

import java.util.UUID;

import javax.servlet.http.HttpSession;

public class CSRFGuardHelper {
    public static final String CSRF_TOKEN_P = "CSRF_TOKEN";

    public static String getToken(HttpSession hs) {
        if (hs == null) {
            return null;
        }
        Object tokenValueFromSession = hs.getAttribute(CSRF_TOKEN_P);
        if (tokenValueFromSession != null) {
            return tokenValueFromSession.toString();
        }

        String newTokenValue = UUID.randomUUID().toString();
        hs.setAttribute(CSRF_TOKEN_P, newTokenValue);
        return newTokenValue;
    }

}
