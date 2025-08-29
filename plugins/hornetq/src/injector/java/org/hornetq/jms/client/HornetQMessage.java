package org.hornetq.jms.client;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.agent.StringUtils;

import javax.jms.Destination;
import javax.jms.TextMessage;

public abstract class HornetQMessage implements javax.jms.Message {
    protected static void dumpMessage$profiler(HornetQMessage msg) {
        String destination = null;
        String replyTo = null;
        String messageId = null;
        String correlationId = null;
        long timestamp = 0;
        String text = null, textFragment = null;

        try {
            Profiler.enter("void org.hornetq.jms.client.HornetQMessage.dumpMessage$profiler(MessageListener,Message) (HornetQMessage.java:9) [unknown.jar]");
            final Destination messageDestination = msg.getJMSDestination();
            if (messageDestination != null)
                destination = messageDestination.toString();
            final Destination replyDestination = msg.getJMSReplyTo();
            if (replyDestination != null)
                replyTo = replyDestination.toString();
            messageId = msg.getJMSMessageID();
            correlationId = msg.getJMSCorrelationID();
            timestamp = msg.getJMSTimestamp();

            if ((msg instanceof TextMessage) && ProfilerData.LOG_JMS_TEXT) {
                TextMessage textMessage = (TextMessage) msg;
                text = textMessage.getText();
                if (text == null || text.length() <= 1500) {
                    textFragment = text;
                    text = null;
                } else textFragment = text.substring(0, 997) + "...";
            }
        } catch (Throwable t) {
            Profiler.pluginException(t);
        } finally {
            Profiler.exit();
        }
        Profiler.event(messageId, "jms.messageid");
        Profiler.event(correlationId, "jms.correlationid");
        Profiler.event(replyTo, "jms.replyto");
        if (timestamp != 0) // "new Long" is used for jdk 1.4 compatibility
            Profiler.event(timestamp, "jms.timestamp");
        if (destination != null) {
            Profiler.event(destination, "jms.destination");
            Profiler.getState().callInfo.setCliendId("JMS " + StringUtils.right(destination, CallInfo.CLIENT_ID_LENGTH - 4));
        }
        Profiler.event(text, "jms.text");
        Profiler.event(textFragment, "jms.text.fragment");
    }
}
