package org.apache.activemq.artemis.jms.client;

import com.netcracker.profiler.agent.Profiler;

import javax.jms.MessageListener;

public class JMSMessageListenerWrapper {
    private MessageListener listener;

    public void dumpConsumer$profiler() {
        try {
            if (listener != null) {
                String className = listener.getClass().getName();
                int lastDot = className.lastIndexOf('.');
                if (lastDot != -1 && lastDot < (className.length() - 1)) {
                    className = className.substring(lastDot + 1);
                }
                Profiler.event(className, "jms.consumer");
            }
        } catch (Throwable t) {
            //
        }
    }
}
