package org.qubership.profiler.security.csrf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import javax.servlet.http.HttpSession;

public class CSRFGuardHelper {

    private static final Logger log = LoggerFactory.getLogger(CSRFGuardHelper.class);
    public static final String CSRF_TOKEN_P = "CSRF_TOKEN";
    public static final int TOKEN_LENGTH = 24;

    public static String getToken(HttpSession hs) {
        if (hs != null) {
            Object tokenValueFromSession = hs.getAttribute(CSRF_TOKEN_P);
            if (tokenValueFromSession != null) {
                return tokenValueFromSession.toString();
            } else {
                String newTokenValue = null;
                try {
                    newTokenValue = RandomTokenGenerator.generateRandomId(SecureRandom.getInstance("SHA1PRNG", "SUN"), TOKEN_LENGTH);
                } catch (NoSuchAlgorithmException e) {
                    log.error("CSRF: ", e);
                } catch (NoSuchProviderException e) {
                    log.error("CSRF: ", e);
                }
                hs.setAttribute(CSRF_TOKEN_P, newTokenValue);
                return newTokenValue;
            }
        } else {
            log.error("CSRF: session is empty");
            return null;
        }
    }

}
