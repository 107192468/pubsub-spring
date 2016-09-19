package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.common.TopicPartition;
import com.wanda.ffan.sub.listener.ConsumerSeekAware;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Created by zhangling on 2016/9/15.
 */
public  abstract class AbstractMessageListenerAdapter<T> implements ConsumerSeekAware {


    protected final T delegate;

    private final ConsumerSeekAware seekAware;

    public AbstractMessageListenerAdapter(T delegate) {
        this.delegate = delegate;
        if (delegate instanceof ConsumerSeekAware) {
            this.seekAware = (ConsumerSeekAware) delegate;
        }
        else {
            this.seekAware = null;
        }
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        if (this.seekAware != null) {
            this.seekAware.registerSeekCallback(callback);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (this.seekAware != null) {
            this.seekAware.onPartitionsAssigned(assignments, callback);
        }
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (this.seekAware != null) {
            this.seekAware.onIdleContainer(assignments, callback);
        }
    }
}
