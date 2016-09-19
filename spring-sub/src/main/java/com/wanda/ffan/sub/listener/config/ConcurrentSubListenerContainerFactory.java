package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.common.TopicPartitionInitialOffset;
import com.wanda.ffan.sub.listener.ConcurrentMessageListenerContainer;

import java.util.Collection;

/**
 * Created by zhangling on 2016/9/16.
 */
public class ConcurrentSubListenerContainerFactory extends AbstractSubListenerContainerFactory<ConcurrentMessageListenerContainer>{

    private Integer concurrency;

    /**
     * Specify the container concurrency.
     * @param concurrency the number of consumers to create.
     * @see ConcurrentMessageListenerContainer#setConcurrency(int)
     */
    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    @Override
    protected ConcurrentMessageListenerContainer createContainerInstance(SubListenerEndpoint endpoint) {
        Collection<TopicPartitionInitialOffset> topicPartitions = endpoint.getTopicPartitions();
        if (!topicPartitions.isEmpty()) {
            ContainerProperties properties = new ContainerProperties(
                    topicPartitions.toArray(new TopicPartitionInitialOffset[topicPartitions.size()]));
            return new ConcurrentMessageListenerContainer(getConsumerFactory(), properties);
        }
        else {
            Collection<String> topics = endpoint.getTopics();
            if (!topics.isEmpty()) {
                ContainerProperties properties = new ContainerProperties(topics.toArray(new String[topics.size()]));
                return new ConcurrentMessageListenerContainer(getConsumerFactory(), properties);
            }
            else {
                ContainerProperties properties = new ContainerProperties(endpoint.getTopicPattern());
                return new ConcurrentMessageListenerContainer(getConsumerFactory(), properties);
            }
        }
    }

    @Override
    protected void initializeContainer(ConcurrentMessageListenerContainer instance) {
        super.initializeContainer(instance);
        if (this.concurrency != null) {
            instance.setConcurrency(this.concurrency);
        }
    }
}
