package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.consumer.ConsumerRecord;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface RecordFilterStrategy {
    /**
     * Return true if the record should be discarded.
     * @param consumerRecord the record.
     * @return true to discard.
     */
    boolean filter(ConsumerRecord consumerRecord);
}
