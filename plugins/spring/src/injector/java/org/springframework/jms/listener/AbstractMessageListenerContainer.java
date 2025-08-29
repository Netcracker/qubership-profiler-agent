package org.springframework.jms.listener;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.agent.StringUtils;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.TextMessage;

public class AbstractMessageListenerContainer {
    public native Object getMessageListener();

    private void dumpMessage$profiler(Message message) {
        String consumer = null;
        String destination = null;
        String replyTo = null;
        String messageId = null;
        String correlationId = null;
        long timestamp = 0;
        String unitOfOrder = null;
        String text = null, textFragment = null;

        try {
            Profiler.enter("void weblogic.jms.client.JMSSession.dumpMessage$profiler(MessageListener,Message) (JMSSession.java:9) [unknown.jar]");
            consumer = getMessageListener() == null ? null : String.valueOf(getMessageListener());
            final Destination messageDestination = message.getJMSDestination();
            if (messageDestination != null)
                destination = messageDestination.toString();
            final Destination replyDestination = message.getJMSReplyTo();
            if (replyDestination != null)
                replyTo = replyDestination.toString();
            messageId = message.getJMSMessageID();
            correlationId = message.getJMSCorrelationID();
            timestamp = message.getJMSTimestamp();
            unitOfOrder = message.getStringProperty("JMS_BEA_UnitOfOrder");

            if ((message instanceof TextMessage) && ProfilerData.LOG_JMS_TEXT) {
                TextMessage textMessage = (TextMessage) message;
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
        Profiler.event(consumer, "jms.consumer");
        Profiler.event(messageId, "jms.messageid");
        Profiler.event(correlationId, "jms.correlationid");
        Profiler.event(replyTo, "jms.replyto");
        if (timestamp != 0) // "new Long" is used for jdk 1.4 compatibility
            Profiler.event(timestamp, "jms.timestamp");
        if (destination != null) {
            Profiler.event(destination, "jms.destination");
            Profiler.getState().callInfo.setCliendId("JMS " + StringUtils.right(destination, CallInfo.CLIENT_ID_LENGTH - 4));
        }
        if (unitOfOrder != null && unitOfOrder.length() > 0)
            Profiler.event(unitOfOrder, "jms.unitoforder");
        Profiler.event(text, "jms.text");
        Profiler.event(textFragment, "jms.text.fragment");
    }
}
