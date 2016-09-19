package com.wanda.ffan.sub.listener.support.converter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.Acknowledgment;
import org.springframework.messaging.Message;

import java.lang.reflect.Type;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface RecordMessageConverter extends MessageConverter  {

    /**
     * Convert a {@link ConsumerRecord} to a {@link Message}.
     * @param record the record.
     * @param acknowledgment the acknowledgment.
     * @param payloadType the required payload type.
     * @return the message.
     */
    Message<?> toMessage(ConsumerRecord record, Acknowledgment acknowledgment, Type payloadType);

    /**
     * Convert a message to a producer record.
     * @param message the message.
     * @param defaultTopic the default topic to use if no header found.
     * @return the producer record.
     *
     * TODO
     */
//    ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic);
}
