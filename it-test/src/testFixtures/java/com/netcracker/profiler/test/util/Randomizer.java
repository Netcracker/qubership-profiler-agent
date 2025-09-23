package com.netcracker.profiler.test.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Uses dedicated random generator with optional seed for repeatable testing
 */
public class Randomizer {
    private static final Logger log = LoggerFactory.getLogger(Randomizer.class);
    private static final long SEED = Long.getLong(Randomizer.class.getName() + ".seed", new Random().nextLong());
    public static final Random rnd = new Random(SEED);

    static {
        log.info("Initialized random number generator with seed {}", SEED);
    }

    public static String randomString() {
        return Long.toHexString(Double.doubleToLongBits(Math.random()));
    }
}
