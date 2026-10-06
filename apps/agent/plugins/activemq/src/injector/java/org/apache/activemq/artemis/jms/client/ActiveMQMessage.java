package org.apache.activemq.artemis.jms.client;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.StringUtils;

import javax.jms.Destination;
import javax.jms.Message;

public abstract class ActiveMQMessage implements Message {
    public void dumpMessage$profiler() {
        // ActiveMQMessage
        // +- ActiveMQBytesMessage (doesn't call parent)
        // +- ActiveMQMapMessage (calls parent)
        // +- ActiveMQObjectMessage (calls parent)
        // +- ActiveMQTextMessage (calls parent)

        String messageID = null;
        String correlationID = null;
        long timestamp = 0;
        String groupID = null;
        String destination = null;
        String replyTo = null;
        try {
            messageID = getJMSMessageID();
            correlationID = getJMSCorrelationID();
            timestamp = getJMSTimestamp();
            groupID = getStringProperty("JMSXGroupID");
            Destination jmsDestination = getJMSDestination();
            if (jmsDestination != null) {
                destination = jmsDestination.toString();
            }
            Destination jmsReplyTo = getJMSReplyTo();
            if (jmsReplyTo != null) {
                replyTo = jmsReplyTo.toString();
            }
        } catch (Throwable t) {
            Profiler.event(StringUtils.throwableToString(t), "exception");
        }

        if (messageID != null && !messageID.isEmpty()) {
            Profiler.event(messageID, "jms.messageid");
        }
        if (correlationID != null && !correlationID.isEmpty()) {
            Profiler.event(correlationID, "jms.correlationid");
        }
        if (timestamp > 0) {
            Profiler.event(timestamp, "jms.timestamp");
        }
        if (groupID != null && !groupID.isEmpty()) {
            Profiler.event(groupID, "jms.unitoforder");
        }
        if (destination != null && !destination.isEmpty()) {
            Profiler.event(destination, "jms.destination");
        }
        if (replyTo != null && !replyTo.isEmpty()) {
            Profiler.event(replyTo, "jms.replyto");
        }
    }
}
