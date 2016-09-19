package com.wanda.ffan.sub.listener.support;

/**
 *
 * 这个class目前没什么用途，因为该项目只是做pubsubhttp的封装，并且该模块只做sub
 * Created by zhangling on 2016/9/15.
 */
public abstract class SubHeaders {


    private static final String PREFIX = "pubsub_";


    public static final String TOPIC = PREFIX + "topic";


    public static final String MESSAGE_KEY = PREFIX + "messageKey";


    public static final String PARTITION_ID = PREFIX + "partitionId";


    public static final String OFFSET = PREFIX + "offset";


    public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";


    public static final String RECEIVED_TOPIC = PREFIX + "receivedTopic";


    public static final String RECEIVED_MESSAGE_KEY = PREFIX + "receivedMessageKey";


    public static final String RECEIVED_PARTITION_ID = PREFIX + "receivedPartitionId";
}
