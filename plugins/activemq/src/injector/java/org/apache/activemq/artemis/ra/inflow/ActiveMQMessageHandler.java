package org.apache.activemq.artemis.ra.inflow;

import org.qubership.profiler.agent.Profiler;

import javax.resource.spi.endpoint.MessageEndpointFactory;

public class ActiveMQMessageHandler {
    private ActiveMQActivation activation;

    public void dumpConsumer$profiler() {
        try {
            if (activation != null) {
                // It's difficult to get MDB name from the handler, so for now we will just use listener class instead
                MessageEndpointFactory endpointFactory = activation.getMessageEndpointFactory();
                if (endpointFactory != null) {
                    Class<?> endpointClass = (Class<?>) endpointFactory.getClass().getMethod("getEndpointClass").invoke(endpointFactory);
                    if (endpointClass != null) {
                        String className = endpointClass.getName();
                        int lastDot = className.lastIndexOf('.');
                        if (lastDot != -1 && lastDot < (className.length() - 1)) {
                            className = className.substring(lastDot + 1);
                        }
                        Profiler.event(className, "jms.consumer");
                    }
                }
            }
        } catch (Throwable t) {
            //
        }
    }
}
