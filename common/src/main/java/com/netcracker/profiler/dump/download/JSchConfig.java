package com.netcracker.profiler.dump.download;

import com.jcraft.jsch.Session;

public class JSchConfig {
    public static void applyToSession(Session session) {
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
    }
}
