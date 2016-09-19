package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.exception.PubSubException;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangling on 2016/9/15.
 */
public class DelegatingInvocableHandler {

    private final List<InvocableHandlerMethod> handlers;

    private final ConcurrentMap<Class<?>, InvocableHandlerMethod> cachedHandlers =
            new ConcurrentHashMap<Class<?>, InvocableHandlerMethod>();

    private final Object bean;

    /**
     * Construct an instance with the supplied handlers for the bean.
     * @param handlers the handlers.
     * @param bean the bean.
     */
    public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers, Object bean) {
        this.handlers = new ArrayList<InvocableHandlerMethod>(handlers);
        this.bean = bean;
    }

    /**
     * Return the bean for this handler.
     * @return the bean.
     */
    public Object getBean() {
        return this.bean;
    }

    /**
     * Invoke the method with the given message.
     * @param message the message.
     * @param providedArgs additional arguments.
     * @return the result of the invocation.
     * @throws Exception raised if no suitable argument resolver can be found,
     * or the method raised an exception.
     */
    public Object invoke(Message<?> message, Object... providedArgs) throws Exception { //NOSONAR
        Class<? extends Object> payloadClass = message.getPayload().getClass();
        InvocableHandlerMethod handler = getHandlerForPayload(payloadClass);
        return handler.invoke(message, providedArgs);
    }

    /**
     * Determine the {@link InvocableHandlerMethod} for the provided type.
     * @param payloadClass the payload class.
     * @return the handler.
     */
    protected InvocableHandlerMethod getHandlerForPayload(Class<? extends Object> payloadClass) {
        InvocableHandlerMethod handler = this.cachedHandlers.get(payloadClass);
        if (handler == null) {
            handler = findHandlerForPayload(payloadClass);
            if (handler == null) {
                throw new PubSubException("No method found for " + payloadClass);
            }
            this.cachedHandlers.putIfAbsent(payloadClass, handler); //NOSONAR
        }
        return handler;
    }

    protected InvocableHandlerMethod findHandlerForPayload(Class<? extends Object> payloadClass) {
        InvocableHandlerMethod result = null;
        for (InvocableHandlerMethod handler : this.handlers) {
            if (matchHandlerMethod(payloadClass, handler)) {
                if (result != null) {
                    throw new PubSubException("Ambiguous methods for payload type: " + payloadClass + ": " +
                            result.getMethod().getName() + " and " + handler.getMethod().getName());
                }
                result = handler;
            }
        }
        return result;
    }

    protected boolean matchHandlerMethod(Class<? extends Object> payloadClass, InvocableHandlerMethod handler) {
        Method method = handler.getMethod();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        // Single param; no annotation or @Payload
        if (parameterAnnotations.length == 1) {
            MethodParameter methodParameter = new MethodParameter(method, 0);
            if (methodParameter.getParameterAnnotations().length == 0 || methodParameter.hasParameterAnnotation(Payload.class)) {
                if (methodParameter.getParameterType().isAssignableFrom(payloadClass)) {
                    return true;
                }
            }
        }
        boolean foundCandidate = false;
        for (int i = 0; i < parameterAnnotations.length; i++) {
            MethodParameter methodParameter = new MethodParameter(method, i);
            if (methodParameter.getParameterAnnotations().length == 0 || methodParameter.hasParameterAnnotation(Payload.class)) {
                if (methodParameter.getParameterType().isAssignableFrom(payloadClass)) {
                    if (foundCandidate) {
                        throw new PubSubException("Ambiguous payload parameter for " + method.toGenericString());
                    }
                    foundCandidate = true;
                }
            }
        }
        return foundCandidate;
    }

    /**
     * Return a string representation of the method that will be invoked for this payload.
     * @param payload the payload.
     * @return the method name.
     */
    public String getMethodNameFor(Object payload) {
        InvocableHandlerMethod handlerForPayload = getHandlerForPayload(payload.getClass());
        return handlerForPayload == null ? "no match" : handlerForPayload.getMethod().toGenericString(); //NOSONAR
    }
}
