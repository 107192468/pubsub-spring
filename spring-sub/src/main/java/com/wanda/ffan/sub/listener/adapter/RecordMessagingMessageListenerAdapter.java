package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.AcknowledgingMessageListener;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.MessageListener;
import org.springframework.messaging.Message;

import java.lang.reflect.Method;

/**
 * Created by zhangling on 2016/9/16.
 */
public class RecordMessagingMessageListenerAdapter extends MessagingMessageListenerAdapter implements MessageListener, AcknowledgingMessageListener {

    public RecordMessagingMessageListenerAdapter(Object bean, Method method) {
        super(bean, method);
    }

    /**
     * Kafka {@link MessageListener} entry point.
     * <p> Delegate the message to the target listener method,
     * with appropriate conversion of the message argument.
     * @param record the incoming Kafka {@link ConsumerRecord}.
     */
    @Override
    public void onMessage(ConsumerRecord record) {
        onMessage(record, null);
    }

    @Override
    public void onMessage(ConsumerRecord record, Acknowledgment acknowledgment) {
        Message<?> message = toMessagingMessage(record, acknowledgment);
        if (logger.isDebugEnabled()) {
            logger.debug("Processing [" + message + "]");
        }
        invokeHandler(record, acknowledgment, message);
    }
}
