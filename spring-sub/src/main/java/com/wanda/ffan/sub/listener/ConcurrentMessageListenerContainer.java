package com.wanda.ffan.sub.listener;

import com.wanda.ffan.common.TopicPartitionInitialOffset;
import com.wanda.ffan.consumer.ConsumerFactory;
import com.wanda.ffan.sub.listener.config.ContainerProperties;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangling on 2016/9/16.
 */
public class ConcurrentMessageListenerContainer extends AbstractMessageListenerContainer {

    private final ConsumerFactory consumerFactory;

    private final List<SubMessageListenerContainer> containers = new ArrayList<>();

    private int concurrency = 1;

    /**
     * Construct an instance with the supplied configuration properties.
     * The topic partitions are distributed evenly across the delegate
     * {@link SubMessageListenerContainer}s.
     * @param consumerFactory the consumer factory.
     * @param containerProperties the container properties.
     */
    public ConcurrentMessageListenerContainer(ConsumerFactory consumerFactory,
                                              ContainerProperties containerProperties) {
        super(containerProperties);
        Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
        this.consumerFactory = consumerFactory;
    }

    public int getConcurrency() {
        return this.concurrency;
    }

    /**
     * The maximum number of concurrent {@link SubMessageListenerContainer}s running.
     * Messages from within the same partition will be processed sequentially.
     * @param concurrency the concurrency.
     */
    public void setConcurrency(int concurrency) {
        Assert.isTrue(concurrency > 0, "concurrency must be greater than 0");
        this.concurrency = concurrency;
    }

    /**
     * Return the list of {@link SubMessageListenerContainer}s created by
     * this container.
     * @return the list of {@link SubMessageListenerContainer}s created by
     * this container.
     */
    public List<SubMessageListenerContainer> getContainers() {
        return Collections.unmodifiableList(this.containers);
    }

    /*
     * Under lifecycle lock.
     */
    @Override
    protected void doStart() {
        if (!isRunning()) {
            ContainerProperties containerProperties = getContainerProperties();
            TopicPartitionInitialOffset[] topicPartitions = containerProperties.getTopicPartitions();
            if (topicPartitions != null
                    && this.concurrency > topicPartitions.length) {
                this.logger.warn("When specific partitions are provided, the concurrency must be less than or "
                        + "equal to the number of partitions; reduced from " + this.concurrency + " to "
                        + topicPartitions.length);
                this.concurrency = topicPartitions.length;
            }
            setRunning(true);

            for (int i = 0; i < this.concurrency; i++) {
                SubMessageListenerContainer container;
                if (topicPartitions == null) {
                    container = new SubMessageListenerContainer(this.consumerFactory, containerProperties);
                }
                else {
                    container = new SubMessageListenerContainer(this.consumerFactory, containerProperties,
                            partitionSubset(containerProperties, i));
                }
                if (getBeanName() != null) {
                    container.setBeanName(getBeanName() + "-" + i);
                }
                if (getApplicationEventPublisher() != null) {
                    container.setApplicationEventPublisher(getApplicationEventPublisher());
                }
                container.start();
                this.containers.add(container);
            }
        }
    }

    private TopicPartitionInitialOffset[] partitionSubset(ContainerProperties containerProperties, int i) {
        TopicPartitionInitialOffset[] topicPartitions = containerProperties.getTopicPartitions();
        if (this.concurrency == 1) {
            return topicPartitions;
        }
        else {
            int numPartitions = topicPartitions.length;
            if (numPartitions == this.concurrency) {
                return new TopicPartitionInitialOffset[] { topicPartitions[i] };
            }
            else {
                int perContainer = numPartitions / this.concurrency;
                TopicPartitionInitialOffset[] subset;
                if (i == this.concurrency - 1) {
                    subset = Arrays.copyOfRange(topicPartitions, i * perContainer, topicPartitions.length);
                }
                else {
                    subset = Arrays.copyOfRange(topicPartitions, i * perContainer, (i + 1) * perContainer);
                }
                return subset;
            }
        }
    }

    /*
     * Under lifecycle lock.
     */
    @Override
    protected void doStop(final Runnable callback) {
        final AtomicInteger count = new AtomicInteger();
        if (isRunning()) {
            setRunning(false);
            for (SubMessageListenerContainer container : this.containers) {
                if (container.isRunning()) {
                    count.incrementAndGet();
                }
            }
            for (SubMessageListenerContainer container : this.containers) {
                if (container.isRunning()) {
                    container.stop(new Runnable() {

                        @Override
                        public void run() {
                            if (count.decrementAndGet() <= 0) {
                                callback.run();
                            }
                        }

                    });
                }
            }
            this.containers.clear();
        }
    }

    @Override
    public String toString() {
        return "ConcurrentMessageListenerContainer [concurrency=" + this.concurrency + ", beanName="
                + this.getBeanName() + ", running=" + this.isRunning() + "]";
    }
}
