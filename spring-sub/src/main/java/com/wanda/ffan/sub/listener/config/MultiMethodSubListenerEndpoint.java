package com.wanda.ffan.sub.listener.config;

import com.wanda.ffan.sub.listener.adapter.DelegatingInvocableHandler;
import com.wanda.ffan.sub.listener.adapter.HandlerAdapter;
import com.wanda.ffan.sub.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangling on 2016/9/15.
 */
public class MultiMethodSubListenerEndpoint extends MethodSubListenerEndpoint {

    private final List<Method> methods;

    public MultiMethodSubListenerEndpoint(List<Method> methods, Object bean) {
        this.methods = methods;
        setBean(bean);
    }

    @Override
    protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter messageListener) {
        List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<InvocableHandlerMethod>();
        for (Method method : this.methods) {
            invocableHandlerMethods.add(getMessageHandlerMethodFactory()
                    .createInvocableHandlerMethod(getBean(), method));
        }
        DelegatingInvocableHandler delegatingHandler =
                new DelegatingInvocableHandler(invocableHandlerMethods, getBean());
        return new HandlerAdapter(delegatingHandler);
    }
}
