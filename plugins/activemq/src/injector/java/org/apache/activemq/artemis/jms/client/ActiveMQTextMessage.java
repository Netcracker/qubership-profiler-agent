package org.apache.activemq.artemis.jms.client;

import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.agent.StringUtils;

import javax.jms.TextMessage;

public abstract class ActiveMQTextMessage extends ActiveMQMessage implements TextMessage {
    public void dumpTextMessage$profiler() {
        if (ProfilerData.LOG_JMS_TEXT) {
            String text = null;
            String textFragment = null;
            try {
                text = getText();
                if (text != null && !text.isEmpty()) {
                    if (text.length() < 1500) {
                        textFragment = text;
                        text = null;
                    } else {
                        textFragment = text.substring(0, 997) + "...";
                    }
                }
            } catch (Throwable t) {
                Profiler.event(StringUtils.throwableToString(t), "exception");
            }

            if (text != null && !text.isEmpty()) {
                Profiler.event(text, "jms.text");
            }
            if (textFragment != null) {
                Profiler.event(textFragment, "jms.text.fragment");
            }
        }
    }
}
