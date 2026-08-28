package com.netcracker.profiler.security;

import static com.netcracker.profiler.security.SecurityConstants.USERNAME_EV;
import static com.netcracker.profiler.security.SecurityConstants.USER_PASSWORD_EV;
import static com.netcracker.profiler.util.StringUtils.isNotBlank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class DummySecurityService {
    private static final Logger logger = LoggerFactory.getLogger(DummySecurityService.class);

    private boolean securityEnabled = false;
    private User defaultUser;

    @Inject
    public DummySecurityService() {
        initialize();
    }

    public User tryAuthenticate(String userName, String password) throws WinstoneAuthException {
        if(!securityEnabled) {
            throw new IllegalStateException("Dummy security is disabled");
        }
        String encodedPassword = encryptPassword(password);
        if (defaultUser.getName().equals(userName) && defaultUser.getEncodedPassword().equals(encodedPassword)) {
            return defaultUser;
        }
        throw new WinstoneAuthException("There is no user with the given name and password");
    }

    private void initialize() {
        String defaultUserName = System.getenv(USERNAME_EV);
        String defaultUserPassword = System.getenv(USER_PASSWORD_EV);
        if (isNotBlank(defaultUserName) && isNotBlank(defaultUserPassword)) {
            logger.info("DummySecurity is enabled");
            defaultUser = new User();
            defaultUser.setName(defaultUserName);
            defaultUser.setEncodedPassword(encryptPassword(defaultUserPassword));
            securityEnabled = true;
        } else {
            logger.info("Dummy security is disabled");
        }
    }

    public boolean isSecurityEnabled() {
        return securityEnabled;
    }

    private String encryptPassword(String password) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(password.getBytes());
            return new BigInteger(1, md5.digest()).toString(16).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Impossible to encrypt password", e);
        }
    }
}
