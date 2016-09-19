package com.wanda.ffan.sub.listener;

import com.wanda.ffan.config.Http;
import com.wanda.ffan.config.Sub;
import com.wanda.ffan.consumer.Consumer;
import com.wanda.ffan.consumer.ConsumerFactory;
import com.wanda.ffan.sub.SubConsumer;
import org.apache.http.impl.client.CloseableHttpClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangling on 2016/9/16.
 */
public class DefaultSubConsumerFactory implements ConsumerFactory {
    private final Http defaultHttp;
    private final Sub defaultSub;
    private final CloseableHttpClient defaultHttpClient;


    public DefaultSubConsumerFactory(Http defaultHttp,Sub defaultSub,CloseableHttpClient defaultHttpClient) {
        this.defaultSub=defaultSub;
        this.defaultHttp=defaultHttp;
        this.defaultHttpClient=defaultHttpClient;
    }


    @Override
    public Consumer createConsumer() {
        return createSubConsumer();
    }

    protected SubConsumer createSubConsumer() {
        return new SubConsumer(defaultHttp, defaultSub, defaultHttpClient);
    }

    @Override
    public boolean isAutoCommit() {
       //先认为全部都是自动commit
        return true;
    }
}
