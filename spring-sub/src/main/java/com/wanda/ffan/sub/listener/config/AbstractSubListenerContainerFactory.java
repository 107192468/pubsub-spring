package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.consumer.ConsumerFactory;
import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.AbstractMessageListenerContainer;
import com.wanda.ffan.sub.listener.adapter.RecordFilterStrategy;
import com.wanda.ffan.sub.listener.support.converter.MessageConverter;
import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import java.util.regex.Pattern;

/**
 * Created by zhangling on 2016/9/16.
 */
public abstract class AbstractSubListenerContainerFactory<C extends AbstractMessageListenerContainer>  implements SubListenerContainerFactory<C>,ApplicationEventPublisherAware {

    private final ContainerProperties containerProperties = new ContainerProperties((Pattern) null);

    private ConsumerFactory consumerFactory;

    private Boolean autoStartup;

    private Integer phase;

    private MessageConverter messageConverter;

    private RecordFilterStrategy recordFilterStrategy;

    private Boolean ackDiscarded;

    private final RecordFilterStrategy DefaultRecordFilter=new RecordFilterImpl();

    private Boolean batchListener;

    private ApplicationEventPublisher applicationEventPublisher;

    /**
     * Specify a {@link ConsumerFactory} to use.
     * @param consumerFactory The consumer factory.
     */
    public void setConsumerFactory(ConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public ConsumerFactory getConsumerFactory() {
        return this.consumerFactory;
    }

    /**
     * Specify an {@code autoStartup boolean} flag.
     * @param autoStartup true for auto startup.
     * @see AbstractMessageListenerContainer#setAutoStartup(boolean)
     */
    public void setAutoStartup(Boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    /**
     * Specify a {@code phase} to use.
     * @param phase The phase.
     * @see AbstractMessageListenerContainer#setPhase(int)
     */
    public void setPhase(int phase) {
        this.phase = phase;
    }

    /**
     * Set the message converter to use if dynamic argument type matching is needed.
     * @param messageConverter the converter.
     */
    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    /**
     * Set the record filter strategy.
     * @param recordFilterStrategy the strategy.
     */
    public void setRecordFilterStrategy(RecordFilterStrategy recordFilterStrategy) {
        this.recordFilterStrategy = recordFilterStrategy;
    }

    /**
     * Set to true to ack discards when a filter strategy is in use.
     * @param ackDiscarded the ackDiscarded.
     */
    public void setAckDiscarded(Boolean ackDiscarded) {
        this.ackDiscarded = ackDiscarded;
    }



    /**
     * Return true if this endpoint creates a batch listener.
     * @return true for a batch listener.
     * @since 1.1
     */
    public Boolean isBatchListener() {
        return this.batchListener;
    }

    /**
     * Set to true if this endpoint should create a batch listener.
     * @param batchListener true for a batch listener.
     * @since 1.1
     */
    public void setBatchListener(Boolean batchListener) {
        this.batchListener = batchListener;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Obtain the properties template for this factory - set properties as needed
     * and they will be copied to a final properties instance for the endpoint.
     * @return the properties.
     */
    public ContainerProperties getContainerProperties() {
        return this.containerProperties;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C createListenerContainer(SubListenerEndpoint endpoint) {
        C instance = createContainerInstance(endpoint);

        if (this.autoStartup != null) {
            instance.setAutoStartup(this.autoStartup);
        }
        if (this.phase != null) {
            instance.setPhase(this.phase);
        }
        if (this.applicationEventPublisher != null) {
            instance.setApplicationEventPublisher(this.applicationEventPublisher);
        }
        if (endpoint.getId() != null) {
            instance.setBeanName(endpoint.getId());
        }

        if (endpoint instanceof AbstractSubListenerEndpoint) {
            AbstractSubListenerEndpoint aklEndpoint = (AbstractSubListenerEndpoint) endpoint;
            if (this.recordFilterStrategy != null) {
                aklEndpoint.setRecordFilterStrategy(this.recordFilterStrategy);
            }
            if (this.ackDiscarded != null) {
                aklEndpoint.setAckDiscarded(this.ackDiscarded);
            }

            if (this.batchListener != null) {
                aklEndpoint.setBatchListener(this.batchListener);
            }
        }

        endpoint.setupListenerContainer(instance, this.messageConverter);
        initializeContainer(instance);

        return instance;
    }

    /**
     * Create an empty container instance.
     * @param endpoint the endpoint.
     * @return the new container instance.
     */
    protected abstract C createContainerInstance(SubListenerEndpoint endpoint);

    /**
     * Further initialize the specified container.
     * <p>Subclasses can inherit from this method to apply extra
     * configuration if necessary.
     * @param instance the container instance to configure.
     */
    protected void initializeContainer(C instance) {
        ContainerProperties properties = instance.getContainerProperties();
        BeanUtils.copyProperties(this.containerProperties, properties, "topics", "topicPartitions", "topicPattern",
                "messageListener", "ackCount", "ackTime");
        if (this.containerProperties.getAckCount() > 0) {
            properties.setAckCount(this.containerProperties.getAckCount());
        }
        if (this.containerProperties.getAckTime() > 0) {
            properties.setAckTime(this.containerProperties.getAckTime());
        }
    }

   
    
    private  class RecordFilterImpl implements RecordFilterStrategy {

        private boolean called;

        @Override
        public boolean filter(ConsumerRecord consumerRecord) {
            called = true;
            return false;
        }

    }
}
