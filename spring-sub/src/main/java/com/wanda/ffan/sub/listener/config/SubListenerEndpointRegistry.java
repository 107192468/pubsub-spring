package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.sub.listener.MessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.*;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangling on 2016/9/15.
 */
public class SubListenerEndpointRegistry implements DisposableBean, SmartLifecycle, ApplicationContextAware, ApplicationListener<ContextRefreshedEvent> {

    protected  final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Map<String, MessageListenerContainer> listenerContainers =
            new ConcurrentHashMap<String, MessageListenerContainer>();

    private int phase = Integer.MAX_VALUE;

    private ConfigurableApplicationContext applicationContext;

    private boolean contextRefreshed;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (applicationContext instanceof ConfigurableApplicationContext) {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }
    }

    /**
     * Return the {@link MessageListenerContainer} with the specified id or
     * {@code null} if no such container exists.
     * @param id the id of the container
     * @return the container or {@code null} if no container with that id exists
     * @see SubListenerEndpoint#getId()
     * @see #getListenerContainerIds()
     */
    public MessageListenerContainer getListenerContainer(String id) {
        Assert.hasText(id, "Container identifier must not be empty");
        return this.listenerContainers.get(id);
    }

    /**
     * Return the ids of the managed {@link MessageListenerContainer} instance(s).
     * @return the ids.
     * @see #getListenerContainer(String)
     */
    public Set<String> getListenerContainerIds() {
        return Collections.unmodifiableSet(this.listenerContainers.keySet());
    }

    /**
     * Return the managed {@link MessageListenerContainer} instance(s).
     * @return the managed {@link MessageListenerContainer} instance(s).
     */
    public Collection<MessageListenerContainer> getListenerContainers() {
        return Collections.unmodifiableCollection(this.listenerContainers.values());
    }

    /**
     * Create a message listener container for the given {@link SubListenerEndpoint}.
     * <p>This create the necessary infrastructure to honor that endpoint
     * with regards to its configuration.
     * @param endpoint the endpoint to add
     * @param factory the listener factory to use
     * @see #registerListenerContainer(SubListenerEndpoint,SubListenerContainerFactory, boolean)
     */
    public void registerListenerContainer(SubListenerEndpoint endpoint, SubListenerContainerFactory<?> factory) {
        registerListenerContainer(endpoint, factory, false);
    }

    /**
     * Create a message listener container for the given {@link SubListenerEndpoint}.
     * <p>This create the necessary infrastructure to honor that endpoint
     * with regards to its configuration.
     * <p>The {@code startImmediately} flag determines if the container should be
     * started immediately.
     * @param endpoint the endpoint to add.
     * @param factory the {@link SubListenerContainerFactory} to use.
     * @param startImmediately start the container immediately if necessary
     * @see #getListenerContainers()
     * @see #getListenerContainer(String)
     */
    @SuppressWarnings("unchecked")
    public void registerListenerContainer(SubListenerEndpoint endpoint, SubListenerContainerFactory<?> factory,
                                          boolean startImmediately) {
        Assert.notNull(endpoint, "Endpoint must not be null");
        Assert.notNull(factory, "Factory must not be null");

        String id = endpoint.getId();
        Assert.hasText(id, "Endpoint id must not be empty");
        synchronized (this.listenerContainers) {
            Assert.state(!this.listenerContainers.containsKey(id),
                    "Another endpoint is already registered with id '" + id + "'");
            MessageListenerContainer container = createListenerContainer(endpoint, factory);
            this.listenerContainers.put(id, container);
            if (StringUtils.hasText(endpoint.getGroup()) && this.applicationContext != null) {
                List<MessageListenerContainer> containerGroup;
                if (this.applicationContext.containsBean(endpoint.getGroup())) {
                    containerGroup = this.applicationContext.getBean(endpoint.getGroup(), List.class);
                }
                else {
                    containerGroup = new ArrayList<MessageListenerContainer>();
                    this.applicationContext.getBeanFactory().registerSingleton(endpoint.getGroup(), containerGroup);
                }
                containerGroup.add(container);
            }
            if (startImmediately) {
                startIfNecessary(container);
            }
        }
    }

    /**
     * Create and start a new {@link MessageListenerContainer} using the specified factory.
     * @param endpoint the endpoint to create a {@link MessageListenerContainer}.
     * @param factory the {@link SubListenerContainerFactory} to use.
     * @return the {@link MessageListenerContainer}.
     */
    protected MessageListenerContainer createListenerContainer(SubListenerEndpoint endpoint,
                                                               SubListenerContainerFactory<?> factory) {

        MessageListenerContainer listenerContainer = factory.createListenerContainer(endpoint);

        if (listenerContainer instanceof InitializingBean) {
            try {
                ((InitializingBean) listenerContainer).afterPropertiesSet();
            }
            catch (Exception ex) {
                throw new BeanInitializationException("Failed to initialize message listener container", ex);
            }
        }

        int containerPhase = listenerContainer.getPhase();
        if (containerPhase < Integer.MAX_VALUE) {  // a custom phase value
            if (this.phase < Integer.MAX_VALUE && this.phase != containerPhase) {
                throw new IllegalStateException("Encountered phase mismatch between container factory definitions: " +
                        this.phase + " vs " + containerPhase);
            }
            this.phase = listenerContainer.getPhase();
        }

        return listenerContainer;
    }


    @Override
    public void destroy() {
        for (MessageListenerContainer listenerContainer : getListenerContainers()) {
            if (listenerContainer instanceof DisposableBean) {
                try {
                    ((DisposableBean) listenerContainer).destroy();
                }
                catch (Exception ex) {
                    this.logger.warn("Failed to destroy message listener container", ex);
                }
            }
        }
    }


    // Delegating implementation of SmartLifecycle

    @Override
    public int getPhase() {
        return this.phase;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void start() {
        for (MessageListenerContainer listenerContainer : getListenerContainers()) {
            startIfNecessary(listenerContainer);
        }
    }

    @Override
    public void stop() {
        for (MessageListenerContainer listenerContainer : getListenerContainers()) {
            listenerContainer.stop();
        }
    }

    @Override
    public void stop(Runnable callback) {
        Collection<MessageListenerContainer> listenerContainers = getListenerContainers();
        AggregatingCallback aggregatingCallback = new AggregatingCallback(listenerContainers.size(), callback);
        for (MessageListenerContainer listenerContainer : listenerContainers) {
            if (listenerContainer.isRunning()) {
                listenerContainer.stop(aggregatingCallback);
            }
            else {
                aggregatingCallback.run();
            }
        }
    }

    @Override
    public boolean isRunning() {
        for (MessageListenerContainer listenerContainer : getListenerContainers()) {
            if (listenerContainer.isRunning()) {
                return true;
            }
        }
        return false;
    }


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (event.getApplicationContext().equals(this.applicationContext)) {
            this.contextRefreshed = true;
        }
    }

    /**
     * Start the specified {@link MessageListenerContainer} if it should be started
     * on startup.
     * @param listenerContainer the listener container to start.
     * @see MessageListenerContainer#isAutoStartup()
     */
    private void startIfNecessary(MessageListenerContainer listenerContainer) {
        if (this.contextRefreshed || listenerContainer.isAutoStartup()) {
            listenerContainer.start();
        }
    }


    private static final class AggregatingCallback implements Runnable {

        private final AtomicInteger count;

        private final Runnable finishCallback;

        private AggregatingCallback(int count, Runnable finishCallback) {
            this.count = new AtomicInteger(count);
            this.finishCallback = finishCallback;
        }

        @Override
        public void run() {
            if (this.count.decrementAndGet() <= 0) {
                this.finishCallback.run();
            }
        }

    }


}
