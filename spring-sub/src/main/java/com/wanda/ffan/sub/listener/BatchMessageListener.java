package com.wanda.ffan.sub.listener;

import com.wanda.ffan.consumer.ConsumerRecord;

import java.util.List;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface BatchMessageListener extends GenericMessageListener<List<ConsumerRecord>> {
}
