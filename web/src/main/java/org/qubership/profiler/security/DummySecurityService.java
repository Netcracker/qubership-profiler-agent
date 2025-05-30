package org.qubership.profiler.security;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.qubership.profiler.security.SecurityConstants.USERNAME_EV;
import static org.qubership.profiler.security.SecurityConstants.USER_PASSWORD_EV;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

public class DummySecurityService {
    private boolean securityEnabled = false;
    private User defaultUser;
    private static volatile DummySecurityService instance;
    private static final Logger logger = LoggerFactory.getLogger(DummySecurityService.class);

    public
    User tryAuthenticate(String userName, String password) throws WinstoneAuthException {
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
            byte[] digest = md5.digest();
            return DatatypeConverter.printHexBinary(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Impossible to encrypt password", e);
        }
    }

    public static DummySecurityService getInstance() {
        DummySecurityService localInstance = DummySecurityService.instance;
        if (localInstance == null) {
            synchronized (DummySecurityService.class) {
                localInstance = DummySecurityService.instance;
                if (localInstance == null) {
                    instance = localInstance = new DummySecurityService();
                    localInstance.initialize();
                }
            }
        }
        return localInstance;
    }
}
