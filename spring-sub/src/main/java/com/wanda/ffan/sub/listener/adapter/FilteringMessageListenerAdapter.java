package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.MessageListener;
import com.wanda.ffan.sub.listener.config.AbstractFilteringMessageListener;

/**
 * Created by zhangling on 2016/9/15.
 */
public class FilteringMessageListenerAdapter extends AbstractFilteringMessageListener<MessageListener>
        implements MessageListener  {

    /**
     * Create an instance with the supplied strategy and delegate listener.
     * @param delegate the delegate.
     * @param recordFilterStrategy the filter.
     */
    public FilteringMessageListenerAdapter(MessageListener delegate,
                                           RecordFilterStrategy recordFilterStrategy) {
        super(delegate, recordFilterStrategy);
    }

    @Override
    public void onMessage(ConsumerRecord consumerRecord) {
        if (!filter(consumerRecord)) {
            this.delegate.onMessage(consumerRecord);
        }
    }
}
