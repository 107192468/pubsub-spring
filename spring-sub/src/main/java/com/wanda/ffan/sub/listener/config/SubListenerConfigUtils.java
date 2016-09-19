package com.wanda.ffan.sub.listener.config;

/**
 * Created by zhangling on 2016/9/15.
 */
public class SubListenerConfigUtils {

    /**
     * The bean name of the internally managed Kafka listener annotation processor.
     */
    public static final String SUB_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
            "com.wanda.ffan.sub.listener.config.internalSubListenerAnnotationProcessor";

    /**
     * The bean name of the internally managed Kafka listener endpoint registry.
     */
    public static final String SUB_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
            "com.wanda.ffan.sub.listener.config.internalSubListenerEndpointRegistry";
}
