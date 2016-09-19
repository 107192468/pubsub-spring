package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.adapter.AbstractMessageListenerAdapter;
import com.wanda.ffan.sub.listener.adapter.RecordFilterStrategy;
import org.springframework.util.Assert;

/**
 * Created by zhangling on 2016/9/15.
 */
public class AbstractFilteringMessageListener<T> extends AbstractMessageListenerAdapter<T> {
    private final RecordFilterStrategy recordFilterStrategy;

    protected AbstractFilteringMessageListener(T delegate, RecordFilterStrategy recordFilterStrategy) {
        super(delegate);
        Assert.notNull(recordFilterStrategy, "'recordFilterStrategy' cannot be null");
        this.recordFilterStrategy = recordFilterStrategy;
    }

    protected boolean filter(ConsumerRecord consumerRecord) {
        return this.recordFilterStrategy.filter(consumerRecord);
    }
}
