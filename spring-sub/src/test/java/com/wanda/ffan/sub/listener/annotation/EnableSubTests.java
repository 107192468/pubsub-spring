package com.wanda.ffan.sub.listener.annotation;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.wanda.ffan.config.Http;
import com.wanda.ffan.config.Sub;
import com.wanda.ffan.sub.listener.ConcurrentMessageListenerContainer;
import com.wanda.ffan.sub.listener.DefaultSubConsumerFactory;
import com.wanda.ffan.sub.listener.adapter.RecordFilterStrategy;
import com.wanda.ffan.sub.listener.config.ConcurrentSubListenerContainerFactory;
import com.wanda.ffan.sub.listener.config.SubListenerContainerFactory;
import com.wanda.ffan.sub.listener.config.SubListenerEndpointRegistry;
import com.wanda.ffan.sub.listener.support.SubHeaders;
import com.wanda.ffan.sub.listener.support.SubNull;
import com.wanda.ffan.sub.listener.support.converter.MessagingMessageConverter;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.wanda.ffan.consumer.ConsumerFactory;
import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.sub.listener.event.ListenerContainerIdleEvent;
import com.wanda.ffan.sub.listener.Acknowledgment;
import com.wanda.ffan.sub.listener.MessageListenerContainer;
import org.springframework.util.Assert;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class EnableSubTests {
    @Autowired
    public Listener listener;

    @Autowired
    public SubListenerEndpointRegistry registry;
    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void testAutoStartup() throws Exception {
        MessageListenerContainer listenerContainer = registry.getListenerContainer("manualStart");
        listenerContainer.start();
        this.registry.start();
        Assert.isTrue(this.registry.isRunning());
//        registry.stop();
//		listenerContainer.stop();
    }


    @Configuration
    @EnableSub
    public static class Config {
        @Bean
        public Sub testSub() {
            return new Sub("testSub", "73", "0de28842dab146f5b6b726d1d2d93867", "http://10.213.57.148:10192/topics/73/test/v1?group=risk_beacon");
        }

        @Bean
        public Http testHttp() {
            return new Http();
        }

        @Bean
        public CloseableHttpClient httpClient() {
        	
        	
            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setSocketTimeout(50000)
                    .setConnectTimeout(50000)
                    .setConnectionRequestTimeout(50000)
                    .setStaleConnectionCheckEnabled(true)
                    .build();
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            // 将最大连接数增加到200
            cm.setMaxTotal(200);
            // 将每个路由基础的连接增加到50
            cm.setDefaultMaxPerRoute(50);
            
            CloseableHttpClient httpclient = HttpClients.custom()
                    .setDefaultRequestConfig(defaultRequestConfig)
                    .setConnectionManager(cm)
                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    .setRetryHandler(new DefaultHttpRequestRetryHandler())
                    .build();

            return httpclient;
        }

        @Bean
        public static PropertySourcesPlaceholderConfigurer ppc() {
            return new PropertySourcesPlaceholderConfigurer();
        }


        @Bean
        public SubListenerContainerFactory<ConcurrentMessageListenerContainer>
        SubListenerContainerFactory() {
            ConcurrentSubListenerContainerFactory factory =
                    new ConcurrentSubListenerContainerFactory();
            factory.setConsumerFactory(consumerFactory());
            factory.setMessageConverter(new MessagingMessageConverter());
            factory.setRecordFilterStrategy(recordFilter());
            return factory;
        }


        @Bean
        public RecordFilterImpl recordFilter() {
            return new RecordFilterImpl();
        }

        @Bean
        public RecordFilterImpl manualFilter() {
            return new RecordFilterImpl();
        }


        @Bean
        public ConsumerFactory consumerFactory() {
            return new DefaultSubConsumerFactory(testHttp(), testSub(), httpClient());
        }

        @Bean
        public Listener listener() {
            return new Listener();
        }

        @Bean
        public IfaceListener<String> ifaceListener() {
            return new IfaceListenerImpl();
        }

        @Bean
        public MultiListenerBean multiListener() {
            return new MultiListenerBean();
        }

    }


    static class Listener {

        private final CountDownLatch latch1 = new CountDownLatch(1);

        private final CountDownLatch latch2 = new CountDownLatch(1);

        private final CountDownLatch latch3 = new CountDownLatch(1);

        private final CountDownLatch latch4 = new CountDownLatch(1);

        private final CountDownLatch latch5 = new CountDownLatch(1);

        private final CountDownLatch latch6 = new CountDownLatch(1);

        private final CountDownLatch latch7 = new CountDownLatch(1);

        private final CountDownLatch latch8 = new CountDownLatch(1);

        private final CountDownLatch latch9 = new CountDownLatch(1);

        private final CountDownLatch eventLatch = new CountDownLatch(1);

        private volatile Integer partition;

        private volatile ConsumerRecord record;

        private volatile Acknowledgment ack;

        private Integer key;

        private String topic;

        private Foo foo;

        private volatile ListenerContainerIdleEvent event;

        @SubListener(id = "manualStart", topics = "manualStart",
                containerFactory = "SubListenerContainerFactory")
        public void manualStart(String foo) {

        }

    }

    interface IfaceListener<T> {

        void listen(T foo);

    }


    @SubListener(id = "multi", topics = "annotated8")
    static class MultiListenerBean {

        private final CountDownLatch latch1 = new CountDownLatch(2);

        @SubHandler
        public void bar(String bar) {
            latch1.countDown();
        }

        @SubHandler
        public void bar(@Payload(required = false) SubNull nul, @Header(SubHeaders.RECEIVED_MESSAGE_KEY) int key) {
            latch1.countDown();
        }

        public void foo(String bar) {
        }

    }

    public static class Foo {

        private String bar;

        public String getBar() {
            return this.bar;
        }

        public void setBar(String bar) {
            this.bar = bar;
        }

    }

    public static class RecordFilterImpl implements RecordFilterStrategy {

        private boolean called;

        @Override
        public boolean filter(ConsumerRecord consumerRecord) {
            called = true;
            return false;
        }

    }


    static class IfaceListenerImpl implements IfaceListener<String> {

        private final CountDownLatch latch1 = new CountDownLatch(1);

        private final CountDownLatch latch2 = new CountDownLatch(1);

        @Override
        @SubListener(id = "ifc", topics = "annotated7")
        public void listen(String foo) {
            latch1.countDown();
        }

        @SubListener(id = "ifctx", topics = "annotated9")
        public void listenTx(String foo) {
            latch2.countDown();
        }

        public CountDownLatch getLatch1() {
            return latch1;
        }

        public CountDownLatch getLatch2() {
            return latch2;
        }
    }


}
