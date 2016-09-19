package com.wanda.ffan.sub.listener.annotation;

import com.wanda.ffan.sub.listener.config.SubListenerEndpointRegistrar;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface SubListenerConfigurer {

    void configureSubListeners(SubListenerEndpointRegistrar registrar);
}
