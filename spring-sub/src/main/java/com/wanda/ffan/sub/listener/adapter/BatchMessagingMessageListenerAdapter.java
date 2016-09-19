package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.BatchAcknowledgingMessageListener;
import com.wanda.ffan.sub.listener.BatchMessageListener;
import com.wanda.ffan.sub.listener.support.SubNull;
import com.wanda.ffan.sub.listener.support.converter.BatchMessageConverter;
import com.wanda.ffan.sub.listener.support.converter.BatchMessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangling on 2016/9/15.
 */
public class BatchMessagingMessageListenerAdapter extends MessagingMessageListenerAdapter  implements BatchMessageListener, BatchAcknowledgingMessageListener {
    private static final Message<SubNull> NULL_MESSAGE = new GenericMessage<>(SubNull.INSTANCE);

    private BatchMessageConverter messageConverter = new BatchMessagingMessageConverter();


    public BatchMessagingMessageListenerAdapter(Object bean, Method method) {
        super(bean, method);
    }

    /**
     * Set the BatchMessageConverter.
     * @param messageConverter the converter.
     */
    public void setBatchMessageConverter(BatchMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    /**
     * Return the {@link BatchMessagingMessageConverter} for this listener,
     * being able to convert {@link org.springframework.messaging.Message}.
     * @return the {@link BatchMessagingMessageConverter} for this listener,
     * being able to convert {@link org.springframework.messaging.Message}.
     */
    protected final BatchMessageConverter getBatchMessageConverter() {
        return this.messageConverter;
    }

 
    @Override
    public void onMessage(List<ConsumerRecord> records) {
        onMessage(records, null);
    }

    @Override
    public void onMessage(List<ConsumerRecord> records, Acknowledgment acknowledgment) {
        Message<?> message;
        if (!isConsumerRecordList()) {
            if (isMessageList()) {
                List<Message<?>> messages = new ArrayList<>(records.size());
                for (ConsumerRecord record : records) {
                    messages.add(toMessagingMessage(record, acknowledgment));
                }
                message = MessageBuilder.withPayload(messages).build();
            }
            else {
                message = toMessagingMessage(records, acknowledgment);
            }
        }
        else {
            message = NULL_MESSAGE;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Processing [" + message + "]");
        }
        invokeHandler(records, acknowledgment, message);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Message<?> toMessagingMessage(List records, Acknowledgment acknowledgment) {
        return getBatchMessageConverter().toMessage(records, acknowledgment, this.inferredType);
    }

}
