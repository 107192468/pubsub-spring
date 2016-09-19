package com.wanda.ffan.sub.listener.support.converter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.Acknowledgment;
import org.springframework.messaging.Message;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface BatchMessageConverter extends MessageConverter {
    /**
     * Convert a list of {@link ConsumerRecord} to a {@link Message}.
     * @param records the records.
     * @param acknowledgment the acknowledgment.
     * @param payloadType the required payload type.
     * @return the message.
     */
    Message<?> toMessage(List<ConsumerRecord> records, Acknowledgment acknowledgment, Type payloadType);


}
