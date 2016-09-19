package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.common.TopicPartitionInitialOffset;
import com.wanda.ffan.sub.listener.MessageListenerContainer;
import com.wanda.ffan.sub.listener.support.converter.MessageConverter;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface SubListenerEndpoint {
    /**
     * Return the id of this endpoint.
     * @return the id of this endpoint. The id can be further qualified
     * when the endpoint is resolved against its actual listener
     * container.
     */
    String getId();

    /**
     * Return the group of this endpoint or null if not in a group.
     * @return the group of this endpoint or null if not in a group.
     */
    String getGroup();

    /**
     * Return the topics for this endpoint.
     * @return the topics for this endpoint.
     */
    Collection<String> getTopics();

    /**
     * Return the topicPartitions for this endpoint.
     * @return the topicPartitions for this endpoint.
     */
    Collection<TopicPartitionInitialOffset> getTopicPartitions();

    /**
     * Return the topicPattern for this endpoint.
     * @return the topicPattern for this endpoint.
     */
    Pattern getTopicPattern();

    /**
     * Setup the specified message listener container with the model
     * defined by this endpoint.
     * <p>This endpoint must provide the requested missing option(s) of
     * the specified container to make it usable. Usually, this is about
     * setting the {@code queues} and the {@code messageListener} to
     * use but an implementation may override any default setting that
     * was already set.
     * @param listenerContainer the listener container to configure
     * @param messageConverter the message converter - can be null
     */
    void setupListenerContainer(MessageListenerContainer listenerContainer, MessageConverter messageConverter);

}
