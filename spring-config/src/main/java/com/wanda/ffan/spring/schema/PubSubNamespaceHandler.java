package com.wanda.ffan.spring.schema;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

import com.wanda.ffan.common.utils.ClassHelper;
import com.wanda.ffan.config.Http;
import com.wanda.ffan.config.Pub;
import com.wanda.ffan.config.Sub;



/**
 *  <pub  url="" appid="" key="" />
	<sub url="" appid="" key="" />
	<http maxCon="" rtimeOut="" ctimeOut="" stimeOut=""  idleClose=""/>
 * @author zhangling
 *
 */
public class PubSubNamespaceHandler  extends NamespaceHandlerSupport{

	static {
		ClassHelper.checkDuplicate(PubSubNamespaceHandler.class);
	}

	@Override
	public void init() {
		    registerBeanDefinitionParser("sub", new PubSubBeanDefinitionParser(Sub.class, true));
	        registerBeanDefinitionParser("pub", new PubSubBeanDefinitionParser(Pub.class, true));
	        registerBeanDefinitionParser("http", new PubSubBeanDefinitionParser(Http.class, true));
	}
	

}
