package com.wanda.ffan.sub.listener.adapter;

import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * Created by zhangling on 2016/9/15.
 */
public class HandlerAdapter {

    private final InvocableHandlerMethod invokerHandlerMethod;

    private final DelegatingInvocableHandler delegatingHandler;

    public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
        this.invokerHandlerMethod = invokerHandlerMethod;
        this.delegatingHandler = null;
    }

    public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
        this.invokerHandlerMethod = null;
        this.delegatingHandler = delegatingHandler;
    }

    public Object invoke(Message<?> message, Object... providedArgs) throws Exception { //NOSONAR
        if (this.invokerHandlerMethod != null) {
            return this.invokerHandlerMethod.invoke(message, providedArgs);
        }
        else {
            return this.delegatingHandler.invoke(message, providedArgs);
        }
    }

    public String getMethodAsString(Object payload) {
        if (this.invokerHandlerMethod != null) {
            return this.invokerHandlerMethod.getMethod().toGenericString();
        }
        else {
            return this.delegatingHandler.getMethodNameFor(payload);
        }
    }

    public Object getBean() {
        if (this.invokerHandlerMethod != null) {
            return this.invokerHandlerMethod.getBean();
        }
        else {
            return this.delegatingHandler.getBean();
        }
    }
}
