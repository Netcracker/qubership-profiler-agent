package org.eclipse.osgi.launch;

import org.qubership.profiler.agent.Bootstrap;

import java.util.HashMap;
import java.util.Map;

public class Equinox {
    protected static Map addBootDelegation$profiler(Map configMap) {
        Map writable = configMap == null ? new HashMap() : new HashMap(configMap);
        String bootDelegationProperty = "org.osgi.framework.bootdelegation";
        String delegation = (String) writable.get(bootDelegationProperty);
        for(String profilerPackage : Bootstrap.BOOT_PACKAGES) {
            if (delegation == null) {
                delegation = profilerPackage;
            } else {
                delegation += "," + profilerPackage;
            }
        }
        writable.put(bootDelegationProperty, delegation);
        return writable;
    }
}
