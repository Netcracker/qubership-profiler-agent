package com.netcracker.profiler.agent;

import static com.netcracker.profiler.agent.PropertyFacadeBoot.getPropertyOrEnvVariable;

import java.util.Scanner;
import java.util.logging.Level;

public class ServerNameResolverAgent {
    private static final ESCLogger logger = ESCLogger.getLogger(ServerNameResolverAgent.class.getName());

    public static final String PARAM_POD_NAME = "POD_NAME";
    public static final String DEFAULT_SERVER_NAME = "default";

    public static final String POD_NAME = parsePODName();;
    public static final String SERVER_NAME = System.getProperty("com.netcracker.esc.serverName", System.getProperty("weblogic.Name", System.getProperty("jboss.server.name", POD_NAME)));;
    static {
        if (System.getProperty("com.netcracker.esc.serverName") == null) {
            System.setProperty("com.netcracker.esc.serverName", SERVER_NAME);
        }
    }

    public static String parsePODName() {
        String result = getPropertyOrEnvVariable(PARAM_POD_NAME);
        if (!StringUtils.isBlank(result)) {
            return result;
        }

        try (Scanner s = new Scanner(Runtime.getRuntime().exec("hostname").getInputStream()).useDelimiter("\\A");) {
            if(s.hasNext()) {
                return s.next().trim();
            }
        } catch (Throwable t) {
            logger.log(Level.WARNING, "Exception in getting hostname", t);
        }
        return DEFAULT_SERVER_NAME;
    }

}
