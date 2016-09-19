package com.wanda.ffan.sub.listener.support.converter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.support.SubHeaders;
import com.wanda.ffan.sub.listener.support.SubNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangling on 2016/9/15.
 */
public class BatchMessagingMessageConverter implements BatchMessageConverter {

    private boolean generateMessageId = false;

    private boolean generateTimestamp = false;

    /**
     * Generate {@link Message} {@code ids} for produced messages. If set to {@code false},
     * will try to use a default value. By default set to {@code false}.
     * @param generateMessageId true if a message id should be generated
     */
    public void setGenerateMessageId(boolean generateMessageId) {
        this.generateMessageId = generateMessageId;
    }

    /**
     * Generate {@code timestamp} for produced messages. If set to {@code false}, -1 is
     * used instead. By default set to {@code false}.
     * @param generateTimestamp true if a timestamp should be generated
     */
    public void setGenerateTimestamp(boolean generateTimestamp) {
        this.generateTimestamp = generateTimestamp;
    }

    @Override
    public Message<?> toMessage(List<ConsumerRecord> records, Acknowledgment acknowledgment,
                                Type type) {
        SubMessageHeaders kafkaMessageHeaders = new SubMessageHeaders(this.generateMessageId,
                this.generateTimestamp);

        Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
        List<Object> payloads = new ArrayList<>();
        List<Object> keys = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        List<Integer> partitions = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        rawHeaders.put(SubHeaders.RECEIVED_MESSAGE_KEY, keys);
        rawHeaders.put(SubHeaders.RECEIVED_TOPIC, topics);
        rawHeaders.put(SubHeaders.RECEIVED_PARTITION_ID, partitions);
        rawHeaders.put(SubHeaders.OFFSET, offsets);

        if (acknowledgment != null) {
            rawHeaders.put(SubHeaders.ACKNOWLEDGMENT, acknowledgment);
        }

        for (ConsumerRecord record : records) {
            payloads.add(extractAndConvertValue(record, type));
            keys.add(record.key());
            topics.add(record.topic());
            partitions.add(record.partition());
            offsets.add(record.offset());
        }
        return MessageBuilder.createMessage(payloads, kafkaMessageHeaders);
    }



    /**
     * Subclasses can convert the value; by default, it's returned as provided by Kafka.
     * @param record the record.
     * @param type the required type.
     * @return the value.
     */
    protected Object extractAndConvertValue(ConsumerRecord record, Type type) {
        return record.massage() == null ? SubNull.INSTANCE : record.massage();
    }
}
