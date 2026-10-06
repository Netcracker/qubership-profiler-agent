package org.apache.activemq.artemis.ra.inflow;

import javax.resource.spi.endpoint.MessageEndpointFactory;

public class ActiveMQActivation {
    public native MessageEndpointFactory getMessageEndpointFactory();
}
