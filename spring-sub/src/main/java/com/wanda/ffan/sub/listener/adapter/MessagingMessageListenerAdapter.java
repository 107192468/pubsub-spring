package com.wanda.ffan.sub.listener.adapter;

import com.wanda.ffan.common.TopicPartition;
import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.ConsumerSeekAware;
import com.wanda.ffan.sub.listener.exception.ListenerExecutionFailedException;
import com.wanda.ffan.sub.listener.support.converter.MessagingMessageConverter;
import com.wanda.ffan.sub.listener.support.converter.RecordMessageConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangling on 2016/9/15.
 */
public abstract class MessagingMessageListenerAdapter implements ConsumerSeekAware {
    private final Object bean;
    protected final Type inferredType;
    protected final Log logger = LogFactory.getLog(this.getClass());
    private HandlerAdapter handlerMethod;

    private boolean isConsumerRecordList;

    private boolean isMessageList;

    private RecordMessageConverter messageConverter = new MessagingMessageConverter();


    public MessagingMessageListenerAdapter(Object bean, Method method) {
        this.bean = bean;
        this.inferredType = determineInferredType(method);
    }

    /**
     * Set the MessageConverter.
     * @param messageConverter the converter.
     */
    public void setMessageConverter(RecordMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }


    protected final RecordMessageConverter getMessageConverter() {
        return this.messageConverter;
    }


    public void setHandlerMethod(HandlerAdapter handlerMethod) {
        this.handlerMethod = handlerMethod;
    }

    protected boolean isConsumerRecordList() {
        return this.isConsumerRecordList;
    }

    protected boolean isMessageList() {
        return this.isMessageList;
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        if (this.bean instanceof ConsumerSeekAware) {
            ((ConsumerSeekAware) bean).registerSeekCallback(callback);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (this.bean instanceof ConsumerSeekAware) {
            ((ConsumerSeekAware) bean).onPartitionsAssigned(assignments, callback);
        }
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (this.bean instanceof ConsumerSeekAware) {
            ((ConsumerSeekAware) bean).onIdleContainer(assignments, callback);
        }
    }

    protected Message<?> toMessagingMessage(ConsumerRecord record, Acknowledgment acknowledgment) {
        return getMessageConverter().toMessage(record, acknowledgment, this.inferredType);
    }


    protected final Object invokeHandler(Object data, Acknowledgment acknowledgment, Message<?> message) {
        try {
            if (data instanceof List && !this.isConsumerRecordList) {
                return this.handlerMethod.invoke(message, acknowledgment);
            }
            else {
                return this.handlerMethod.invoke(message, data, acknowledgment);
            }
        }
        catch (org.springframework.messaging.converter.MessageConversionException ex) {
            throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
                    "be invoked with the incoming message", message.getPayload()),
                    new MessageConversionException("Cannot handle message", ex));
        }
        catch (MessagingException ex) {
            throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
                    "be invoked with the incoming message", message.getPayload()), ex);
        }
        catch (Exception ex) {
            throw new ListenerExecutionFailedException("Listener method '" +
                    this.handlerMethod.getMethodAsString(message.getPayload()) + "' threw exception", ex);
        }
    }

    private String createMessagingErrorMessage(String description, Object payload) {
        return description + "\n"
                + "Endpoint handler details:\n"
                + "Method [" + this.handlerMethod.getMethodAsString(payload) + "]\n"
                + "Bean [" + this.handlerMethod.getBean() + "]";
    }

    private Type determineInferredType(Method method) {
        if (method == null) {
            return null;
        }

        Type genericParameterType = null;
        boolean hasAck = false;

        for (int i = 0; i < method.getParameterTypes().length; i++) {
            MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated with @Payload.
			 * We ignore parameters with type Message because they are not involved with conversion.
			 */
            if (eligibleParameter(methodParameter)
                    && (methodParameter.getParameterAnnotations().length == 0
                    || methodParameter.hasParameterAnnotation(Payload.class))) {
                if (genericParameterType == null) {
                    genericParameterType = methodParameter.getGenericParameterType();
                    if (genericParameterType instanceof ParameterizedType) {
                        ParameterizedType parameterizedType = (ParameterizedType) genericParameterType;
                        if (parameterizedType.getRawType().equals(Message.class)) {
                            genericParameterType = ((ParameterizedType) genericParameterType)
                                    .getActualTypeArguments()[0];
                        }
                        else if (parameterizedType.getRawType().equals(List.class)
                                && parameterizedType.getActualTypeArguments().length == 1) {
                            Type paramType = parameterizedType.getActualTypeArguments()[0];
                            this.isConsumerRecordList =	paramType.equals(ConsumerRecord.class)
                                    || (paramType instanceof ParameterizedType
                                    && ((ParameterizedType) paramType).getRawType().equals(ConsumerRecord.class));
                            this.isMessageList = paramType.equals(Message.class)
                                    || (paramType instanceof ParameterizedType
                                    && ((ParameterizedType) paramType).getRawType().equals(Message.class));
                        }
                    }
                }
                else {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug("Ambiguous parameters for target payload for method " + method
                                + "; no inferred type available");
                    }
                    break;
                }
            }
            else if (methodParameter.getGenericParameterType().equals(Acknowledgment.class)) {
                hasAck = true;
            }
        }
        Assert.state(!this.isConsumerRecordList || method.getParameterTypes().length == 1
                        || (method.getGenericParameterTypes().length == 2 && hasAck),
                "A parameter of type 'List<ConsumerRecord>' must be the only parameter "
                        + "(except for an optional 'Acknowledgment')");
        Assert.state(!this.isMessageList || method.getParameterTypes().length == 1
                        || (method.getGenericParameterTypes().length == 2 && hasAck),
                "A parameter of type 'List<Message<?>>' must be the only parameter "
                        + "(except for an optional 'Acknowledgment')");

        return genericParameterType;
    }

    /*
     * Don't consider parameter types that are available after conversion.
     * Acknowledgment, ConsumerRecord and Message<?>.
     */
    private boolean eligibleParameter(MethodParameter methodParameter) {
        Type parameterType = methodParameter.getGenericParameterType();
        if (parameterType.equals(Acknowledgment.class) || parameterType.equals(ConsumerRecord.class)) {
            return false;
        }
        if (parameterType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) parameterType;
            if (parameterizedType.getRawType().equals(Message.class)) {
                return !(parameterizedType.getActualTypeArguments()[0] instanceof WildcardType);
            }
        }
        return !parameterType.equals(Message.class); // could be Message without a generic type
    }
}
