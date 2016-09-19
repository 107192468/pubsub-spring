package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.sub.listener.MessageListenerContainer;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface SubListenerContainerFactory <C extends MessageListenerContainer> {
    /**
     * Create a {@link MessageListenerContainer} for the given {@link SubListenerEndpoint}.
     * @param endpoint the endpoint to configure
     * @return the created container
     */
    C createListenerContainer(SubListenerEndpoint endpoint);
}
