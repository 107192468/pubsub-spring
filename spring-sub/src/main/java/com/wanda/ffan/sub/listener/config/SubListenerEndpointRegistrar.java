package com.wanda.ffan.sub.listener.config;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangling on 2016/9/15.
 */
public class SubListenerEndpointRegistrar implements BeanFactoryAware, InitializingBean {

    private final List<SubListenerEndpointDescriptor> endpointDescriptors =
            new ArrayList<SubListenerEndpointDescriptor>();

    private SubListenerEndpointRegistry endpointRegistry;

    private MessageHandlerMethodFactory messageHandlerMethodFactory;

    private SubListenerContainerFactory<?> containerFactory;

    private String containerFactoryBeanName;

    private BeanFactory beanFactory;

    private boolean startImmediately;



  
    public void setEndpointRegistry(SubListenerEndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
    }

    /**
     * Return the {@link SubListenerEndpointRegistry} instance for this
     * registrar, may be {@code null}.
     * @return the {@link SubListenerEndpointRegistry} instance for this
     * registrar, may be {@code null}.
     */
    public SubListenerEndpointRegistry getEndpointRegistry() {
        return this.endpointRegistry;
    }

    /**
     * Set the {@link MessageHandlerMethodFactory} to use to configure the message
     * listener responsible to serve an endpoint detected by this processor.
     * <p>By default, {@link DefaultMessageHandlerMethodFactory} is used and it
     * can be configured further to support additional method arguments
     * or to customize conversion and validation support. See
     * {@link DefaultMessageHandlerMethodFactory} javadoc for more details.
     * @param SubHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
     */
    public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory SubHandlerMethodFactory) {
        this.messageHandlerMethodFactory = SubHandlerMethodFactory;
    }

    /**
     * Return the custom {@link MessageHandlerMethodFactory} to use, if any.
     * @return the custom {@link MessageHandlerMethodFactory} to use, if any.
     */
    public MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
        return this.messageHandlerMethodFactory;
    }

    /**
     * Set the {@link SubListenerContainerFactory} to use in case a {@link SubListenerEndpoint}
     * is registered with a {@code null} container factory.
     * <p>Alternatively, the bean name of the {@link SubListenerContainerFactory} to use
     * can be specified for a lazy lookup, see {@link #setContainerFactoryBeanName}.
     * @param containerFactory the {@link SubListenerContainerFactory} instance.
     */
    public void setContainerFactory(SubListenerContainerFactory<?> containerFactory) {
        this.containerFactory = containerFactory;
    }

    /**
     * Set the bean name of the {@link SubListenerContainerFactory} to use in case
     * a {@link SubListenerEndpoint} is registered with a {@code null} container factory.
     * Alternatively, the container factory instance can be registered directly:
     * see {@link #setContainerFactory(SubListenerContainerFactory)}.
     * @param containerFactoryBeanName the {@link SubListenerContainerFactory} bean name.
     * @see #setBeanFactory
     */
    public void setContainerFactoryBeanName(String containerFactoryBeanName) {
        this.containerFactoryBeanName = containerFactoryBeanName;
    }

    /**
     * A {@link BeanFactory} only needs to be available in conjunction with
     * {@link #setContainerFactoryBeanName}.
     * @param beanFactory the {@link BeanFactory} instance.
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }


    @Override
    public void afterPropertiesSet() {
        registerAllEndpoints();
    }

    protected void registerAllEndpoints() {
        synchronized (this.endpointDescriptors) {
            for (SubListenerEndpointDescriptor descriptor : this.endpointDescriptors) {
                this.endpointRegistry.registerListenerContainer(
                        descriptor.endpoint, resolveContainerFactory(descriptor));
            }
            this.startImmediately = true;  
        }
    }

    private SubListenerContainerFactory<?> resolveContainerFactory(SubListenerEndpointDescriptor descriptor) {
        if (descriptor.containerFactory != null) {
            return descriptor.containerFactory;
        }
        else if (this.containerFactory != null) {
            return this.containerFactory;
        }
        else if (this.containerFactoryBeanName != null) {
            Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
            this.containerFactory = this.beanFactory.getBean(
                    this.containerFactoryBeanName, SubListenerContainerFactory.class);
            return this.containerFactory;  // Consider changing this if live change of the factory is required
        }
        else {
            throw new IllegalStateException("Could not resolve the " +
                    SubListenerContainerFactory.class.getSimpleName() + " to use for [" +
                    descriptor.endpoint + "] no factory was given and no default is set.");
        }
    }

    /**
     * Register a new {@link SubListenerEndpoint} alongside the
     * {@link SubListenerContainerFactory} to use to create the underlying container.
     * <p>The {@code factory} may be {@code null} if the default factory has to be
     * used for that endpoint.
     * @param endpoint the {@link SubListenerEndpoint} instance to register.
     * @param factory the {@link SubListenerContainerFactory} to use.
     */
    public void registerEndpoint(SubListenerEndpoint endpoint, SubListenerContainerFactory<?> factory) {
        Assert.notNull(endpoint, "Endpoint must be set");
        Assert.hasText(endpoint.getId(), "Endpoint id must be set");
        // Factory may be null, we defer the resolution right before actually creating the container
        SubListenerEndpointDescriptor descriptor = new SubListenerEndpointDescriptor(endpoint, factory);
        synchronized (this.endpointDescriptors) {
            if (this.startImmediately) { // Register and start immediately
                this.endpointRegistry.registerListenerContainer(descriptor.endpoint,
                        resolveContainerFactory(descriptor), true);
            }
            else {
                this.endpointDescriptors.add(descriptor);
            }
        }
    }

    /**
     * Register a new {@link SubListenerEndpoint} using the default
     * {@link SubListenerContainerFactory} to create the underlying container.
     * @param endpoint the {@link SubListenerEndpoint} instance to register.
     * @see #setContainerFactory(SubListenerContainerFactory)
     * @see #registerEndpoint(SubListenerEndpoint, SubListenerContainerFactory)
     */
    public void registerEndpoint(SubListenerEndpoint endpoint) {
        registerEndpoint(endpoint, null);
    }


    private static final class SubListenerEndpointDescriptor {

        private final SubListenerEndpoint endpoint;

        private final SubListenerContainerFactory<?> containerFactory;

        private SubListenerEndpointDescriptor(SubListenerEndpoint endpoint,
                                                SubListenerContainerFactory<?> containerFactory) {
            this.endpoint = endpoint;
            this.containerFactory = containerFactory;
        }

    }
}
