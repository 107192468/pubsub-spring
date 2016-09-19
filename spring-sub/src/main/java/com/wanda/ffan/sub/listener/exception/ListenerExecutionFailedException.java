package com.wanda.ffan.sub.listener.exception;

import com.wanda.ffan.exception.PubSubException;

/**
 * Created by zhangling on 2016/9/15.
 */
public class ListenerExecutionFailedException extends PubSubException{
    public ListenerExecutionFailedException(String message) {
        super(message);
    }

    public ListenerExecutionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
