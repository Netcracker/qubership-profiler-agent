package org.qubership.profiler.security.csrf;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

public final class RandomTokenGenerator {

    private final static char[] CHARSET = new char[]{'A', 'B', 'C', 'D', 'E',
            'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e',
            'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
            '5', '6', '7', '8', '9'};

    private RandomTokenGenerator() {
        /**
         * Intentionally blank to force static usage
         */
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    public static String generateRandomId(String prng, String provider, int len) throws NoSuchAlgorithmException, NoSuchProviderException {
        return generateRandomId(SecureRandom.getInstance(prng, provider), len);
    }

    public static String generateRandomId(SecureRandom sr, int len) {
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i < len + 1; i++) {
            int index = sr.nextInt(CHARSET.length);
            char c = CHARSET[index];
            sb.append(c);

            if ((i % 4) == 0 && i != 0 && i < len) {
                sb.append('-');
            }
        }

        return sb.toString();
    }

}
