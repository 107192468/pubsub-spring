package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.AcknowledgingMessageListener;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.adapter.RecordFilterStrategy;

/**
 * Created by zhangling on 2016/9/15.
 */
public class FilteringAcknowledgingMessageListenerAdapter extends AbstractFilteringMessageListener<AcknowledgingMessageListener>
        implements AcknowledgingMessageListener {

    private final boolean ackDiscarded;

    /**
     * Create an instance with the supplied strategy and delegate listener.
     * @param delegate the delegate.
     * @param recordFilterStrategy the filter.
     * @param ackDiscarded true to ack (commit offset for) discarded messages.
     */
    public FilteringAcknowledgingMessageListenerAdapter(AcknowledgingMessageListener delegate,
                                                        RecordFilterStrategy recordFilterStrategy, boolean ackDiscarded) {
        super(delegate, recordFilterStrategy);
        this.ackDiscarded = ackDiscarded;
    }

    @Override
    public void onMessage(ConsumerRecord consumerRecord, Acknowledgment acknowledgment) {
        if (!filter(consumerRecord)) {
            this.delegate.onMessage(consumerRecord, acknowledgment);
        }
        else {
            if (this.ackDiscarded && acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }
}
