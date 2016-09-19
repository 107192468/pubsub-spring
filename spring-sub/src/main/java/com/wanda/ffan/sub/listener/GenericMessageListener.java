package com.wanda.ffan.sub.listener;

/**
 * Created by zhangling on 2016/9/15.
 */
public interface GenericMessageListener<T> extends SubDataListener<T> {
    void onMessage(T data);
}
