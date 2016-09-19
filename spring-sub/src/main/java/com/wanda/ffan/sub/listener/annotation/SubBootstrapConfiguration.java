/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wanda.ffan.sub.listener.annotation;

import com.wanda.ffan.sub.listener.config.SubListenerConfigUtils;
import com.wanda.ffan.sub.listener.config.SubListenerEndpointRegistry;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;


@Configuration
public class SubBootstrapConfiguration {

	@SuppressWarnings("rawtypes")
	@Bean(name = SubListenerConfigUtils.SUB_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public SubListenerAnnotationBeanPostProcessor SubListenerAnnotationProcessor() {
		return new SubListenerAnnotationBeanPostProcessor();
	}

	@Bean(name =SubListenerConfigUtils.SUB_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
	public SubListenerEndpointRegistry defaultKafkaListenerEndpointRegistry() {
		return new SubListenerEndpointRegistry();
	}

}
