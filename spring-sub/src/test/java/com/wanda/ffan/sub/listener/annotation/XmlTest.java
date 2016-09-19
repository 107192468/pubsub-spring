package com.wanda.ffan.sub.listener.annotation;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.wanda.ffan.config.Sub;


/**
 * PubTest
 * 
 * @author zhangling
 */
public class XmlTest {
    
    @Test
    public void testSpringExtensionInject() {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(XmlTest.class.getPackage().getName().replace('.', '/') + "/spring-extension-inject.xml");
        ctx.start();
        try {
//        	Handler hm=(Handler) ctx.getBean("myPub");
//        	hm.send("test", "dfsfdsfds");
//            MockFilter filter = (MockFilter) ExtensionLoader.getExtensionLoader(Filter.class).getExtension("mymock");
//            assertNotNull(filter.getMockDao());
//            assertNotNull(filter.getProtocol());
//            assertNotNull(filter.getLoadBalance());
        } finally {
            ctx.stop();
            ctx.close();
        }
    }
}