package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.sub.listener.MessageListenerContainer;
import com.wanda.ffan.sub.listener.adapter.BatchMessagingMessageListenerAdapter;
import com.wanda.ffan.sub.listener.adapter.HandlerAdapter;
import com.wanda.ffan.sub.listener.adapter.MessagingMessageListenerAdapter;
import com.wanda.ffan.sub.listener.adapter.RecordMessagingMessageListenerAdapter;
import com.wanda.ffan.sub.listener.support.converter.BatchMessageConverter;
import com.wanda.ffan.sub.listener.support.converter.MessageConverter;
import com.wanda.ffan.sub.listener.support.converter.RecordMessageConverter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

import java.lang.reflect.Method;

/**
 * Created by zhangling on 2016/9/15.
 */
public class MethodSubListenerEndpoint extends AbstractSubListenerEndpoint {
    private Object bean;

    private Method method;

    private MessageHandlerMethodFactory messageHandlerMethodFactory;


    /**
     * Set the object instance that should manage this endpoint.
     * @param bean the target bean instance.
     */
    public void setBean(Object bean) {
        this.bean = bean;
    }

    public Object getBean() {
        return this.bean;
    }

    /**
     * Set the method to invoke to process a message managed by this endpoint.
     * @param method the target method for the {@link #bean}.
     */
    public void setMethod(Method method) {
        this.method = method;
    }

    public Method getMethod() {
        return this.method;
    }

    /**
     * Set the {@link MessageHandlerMethodFactory} to use to build the
     * {@link InvocableHandlerMethod} responsible to manage the invocation
     * of this endpoint.
     * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
     */
    public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
        this.messageHandlerMethodFactory = messageHandlerMethodFactory;
    }

    /**
     * Return the {@link MessageHandlerMethodFactory}.
     * @return the messageHandlerMethodFactory
     */
    protected MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
        return this.messageHandlerMethodFactory;
    }

    @Override
    protected MessagingMessageListenerAdapter createMessageListener(MessageListenerContainer container,
                                                                    MessageConverter messageConverter) {
        Assert.state(this.messageHandlerMethodFactory != null,
                "Could not create message listener - MessageHandlerMethodFactory not set");
        MessagingMessageListenerAdapter messageListener = createMessageListenerInstance(messageConverter);
        messageListener.setHandlerMethod(configureListenerAdapter(messageListener));
        return messageListener;
    }

    /**
     * Create a {@link HandlerAdapter} for this listener adapter.
     * @param messageListener the listener adapter.
     * @return the handler adapter.
     */
    protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter messageListener) {
        InvocableHandlerMethod invocableHandlerMethod =
                this.messageHandlerMethodFactory.createInvocableHandlerMethod(getBean(), getMethod());
        return new HandlerAdapter(invocableHandlerMethod);
    }

    /**
     * Create an empty {@link MessagingMessageListenerAdapter} instance.
     * @param messageConverter the converter (may be null).
     * @return the {@link MessagingMessageListenerAdapter} instance.
     */
    protected MessagingMessageListenerAdapter createMessageListenerInstance(MessageConverter messageConverter) {
        if (isBatchListener()) {
            BatchMessagingMessageListenerAdapter messageListener = new BatchMessagingMessageListenerAdapter(
                    this.bean, this.method);
            if (messageConverter instanceof BatchMessageConverter) {
                messageListener.setBatchMessageConverter((BatchMessageConverter) messageConverter);
            }
            return messageListener;
        }
        else {
            RecordMessagingMessageListenerAdapter messageListener =
                    new RecordMessagingMessageListenerAdapter(this.bean, this.method);
            if (messageConverter instanceof RecordMessageConverter) {
                messageListener.setMessageConverter((RecordMessageConverter) messageConverter);
            }
            return messageListener;
        }
    }

    @Override
    protected StringBuilder getEndpointDescription() {
        return super.getEndpointDescription()
                .append(" | bean='").append(this.bean).append("'")
                .append(" | method='").append(this.method).append("'");
    }
}
