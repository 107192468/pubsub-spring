package com.wanda.ffan.sub.listener.support.converter;

import org.springframework.messaging.MessageHeaders;

import java.util.Map;

/**
 * Created by zhangling on 2016/9/15.
 */
public class SubMessageHeaders extends MessageHeaders {

    SubMessageHeaders(boolean generateId, boolean generateTimestamp) {
        super(null, generateId ? null : ID_VALUE_NONE, generateTimestamp ? null : -1L);
    }

    public Map<String, Object> getRawHeaders() { //NOSONAR - not useless, widening to public
        return super.getRawHeaders();
    }
}
