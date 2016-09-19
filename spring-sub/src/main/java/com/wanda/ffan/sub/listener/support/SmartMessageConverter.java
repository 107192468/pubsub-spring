package com.wanda.ffan.sub.listener.support;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;

/**  spring @since 4.2.1
 * Created by zhangling on 2016/9/16.
 */
public interface SmartMessageConverter extends MessageConverter {
    /**
     * A variant of {@link #fromMessage(Message, Class)} which takes an extra
     * conversion context as an argument, allowing to take e.g. annotations
     * on a payload parameter into account.
     * @param message the input message
     * @param targetClass the target class for the conversion
     * @param conversionHint an extra object passed to the {@link MessageConverter},
     * e.g. the associated {@code MethodParameter} (may be {@code null}}
     * @return the result of the conversion, or {@code null} if the converter cannot
     * perform the conversion
     * @see #fromMessage(Message, Class)
     */
    Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint);

    /**
     * A variant of {@link #toMessage(Object, MessageHeaders)} which takes an extra
     * conversion context as an argument, allowing to take e.g. annotations
     * on a return type into account.
     * @param payload the Object to convert
     * @param headers optional headers for the message (may be {@code null})
     * @param conversionHint an extra object passed to the {@link MessageConverter},
     * e.g. the associated {@code MethodParameter} (may be {@code null}}
     * @return the new message, or {@code null} if the converter does not support the
     * Object type or the target media type
     * @see #toMessage(Object, MessageHeaders)
     */
    Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint);
}
