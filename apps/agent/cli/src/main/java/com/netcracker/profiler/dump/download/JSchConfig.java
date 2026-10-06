package com.netcracker.profiler.dump.download;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.File;

public class JSchConfig {
    public static JSch createJSch() throws JSchException {
        JSch jSch = new JSch();
        String knownHostsFile = System.getProperty("user.home") + File.separator + ".ssh" + File.separator + "known_hosts";
        if (new File(knownHostsFile).exists()) {
            jSch.setKnownHosts(knownHostsFile);
        }
        return jSch;
    }

    public static void applyToSession(Session session) {
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
    }
}
