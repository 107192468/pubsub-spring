package com.wanda.ffan.sub.listener.support.converter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.support.SubHeaders;
import com.wanda.ffan.sub.listener.support.SubNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by zhangling on 2016/9/15.
 */
public class MessagingMessageConverter implements RecordMessageConverter {
    private boolean generateMessageId = false;

    private boolean generateTimestamp = false;
    public void setGenerateMessageId(boolean generateMessageId) {
        this.generateMessageId = generateMessageId;
    }


    public void setGenerateTimestamp(boolean generateTimestamp) {
        this.generateTimestamp = generateTimestamp;
    }

    @Override
    public Message<?> toMessage(ConsumerRecord record, Acknowledgment acknowledgment, Type type) {
        //TODO 查看目前客户端提供的方法
        SubMessageHeaders subMessageHeaders = new SubMessageHeaders(this.generateMessageId,
                this.generateTimestamp);

        Map<String, Object> rawHeaders = subMessageHeaders.getRawHeaders();


        if (acknowledgment != null) {
            rawHeaders.put(SubHeaders.ACKNOWLEDGMENT, acknowledgment);
        }

        return MessageBuilder.createMessage(extractAndConvertValue(record, type), subMessageHeaders);
    }
    protected Object extractAndConvertValue(ConsumerRecord record, Type type) {
            return record.massage() == null ? SubNull.INSTANCE : record.massage();
    }
}
