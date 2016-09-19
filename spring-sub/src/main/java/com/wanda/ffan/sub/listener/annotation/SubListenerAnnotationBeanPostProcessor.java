package com.wanda.ffan.sub.listener.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import com.wanda.ffan.common.TopicPartitionInitialOffset;
import com.wanda.ffan.sub.MethodIntrospector;
import com.wanda.ffan.sub.listener.config.MethodSubListenerEndpoint;
import com.wanda.ffan.sub.listener.config.MultiMethodSubListenerEndpoint;
import com.wanda.ffan.sub.listener.config.SubListenerConfigUtils;
import com.wanda.ffan.sub.listener.config.SubListenerContainerFactory;
import com.wanda.ffan.sub.listener.config.SubListenerEndpointRegistrar;
import com.wanda.ffan.sub.listener.config.SubListenerEndpointRegistry;
/**
 * @author zhangling
*/
public class SubListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, Ordered, BeanFactoryAware, SmartInitializingSingleton {

	
	public static final String DEFAULT_Sub_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "SubListenerContainerFactory";

	private final Set<Class<?>> nonAnnotatedClasses =
			Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>(64));

	private final Log logger = LogFactory.getLog(getClass());

	private SubListenerEndpointRegistry endpointRegistry;

	private String containerFactoryBeanName = DEFAULT_Sub_LISTENER_CONTAINER_FACTORY_BEAN_NAME;

	private BeanFactory beanFactory;

	private final SubHandlerMethodFactoryAdapter messageHandlerMethodFactory =
			new SubHandlerMethodFactoryAdapter();

	private final SubListenerEndpointRegistrar registrar = new SubListenerEndpointRegistrar();

	private final AtomicInteger counter = new AtomicInteger();

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	private BeanExpressionContext expressionContext;

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	/**
	 * Set the {@link SubListenerEndpointRegistry} that will hold the created
	 * endpoint and manage the lifecycle of the related listener container.
	 * @param endpointRegistry the {@link SubListenerEndpointRegistry} to set.
	 */
	public void setEndpointRegistry(SubListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.containerFactoryBeanName = containerFactoryBeanName;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to configure the message
	 * listener responsible to serve an endpoint detected by this processor.
	 * <p>By default, {@link DefaultMessageHandlerMethodFactory} is used and it
	 * can be configured further to support additional method arguments
	 * or to customize conversion and validation support. See
	 * {@link DefaultMessageHandlerMethodFactory} Javadoc for more details.
	 * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
	}


	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
		}
	}


	@Override
	public void afterSingletonsInstantiated() {
		this.registrar.setBeanFactory(this.beanFactory);

		if (this.beanFactory instanceof ListableBeanFactory) {
			Map<String, SubListenerConfigurer> instances =
					((ListableBeanFactory) this.beanFactory).getBeansOfType(SubListenerConfigurer.class);
			for (SubListenerConfigurer configurer : instances.values()) {
				configurer.configureSubListeners(this.registrar);
			}
		}

		if (this.registrar.getEndpointRegistry() == null) {
			if (this.endpointRegistry == null) {
				Assert.state(this.beanFactory != null,
						"BeanFactory must be set to find endpoint registry by bean name");
				this.endpointRegistry = this.beanFactory.getBean(
						SubListenerConfigUtils.SUB_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
						SubListenerEndpointRegistry.class);
			}
			this.registrar.setEndpointRegistry(this.endpointRegistry);
		}

		if (this.containerFactoryBeanName != null) {
			this.registrar.setContainerFactoryBeanName(this.containerFactoryBeanName);
		}

		// Set the custom handler method factory once resolved by the configurer
		MessageHandlerMethodFactory handlerMethodFactory = this.registrar.getMessageHandlerMethodFactory();
		if (handlerMethodFactory != null) {
			this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(handlerMethodFactory);
		}

		// Actually register all listeners
		this.registrar.afterPropertiesSet();
	}


	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Collection<SubListener> classLevelListeners = findListenerAnnotations(targetClass);
			final boolean hasClassLevelListeners = classLevelListeners.size() > 0;
			final List<Method> multiMethods = new ArrayList<Method>();
			Map<Method, Set<SubListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					new MethodIntrospector.MetadataLookup<Set<SubListener>>() {

						@Override
						public Set<SubListener> inspect(Method method) {
							Set<SubListener> listenerMethods = findListenerAnnotations(method);
							return (!listenerMethods.isEmpty() ? listenerMethods : null);
						}

					});
			if (hasClassLevelListeners) {
				Set<Method> methodsWithHandler = MethodIntrospector.selectMethods(targetClass,
						new ReflectionUtils.MethodFilter() {

							@Override
							public boolean matches(Method method) {
								return AnnotationUtils.findAnnotation(method, SubHandler.class) != null;
							}

						});
				multiMethods.addAll(methodsWithHandler);
			}
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("No @SubListener annotations found on bean type: " + bean.getClass());
				}
			}
			else {
				// Non-empty set of methods
				for (Map.Entry<Method, Set<SubListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (SubListener listener : entry.getValue()) {
						processSubListener(listener, method, bean, beanName);
					}
				}
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(annotatedMethods.size() + " @SubListener methods processed on bean '"
							+ beanName + "': " + annotatedMethods);
				}
			}
			if (hasClassLevelListeners) {
				processMultiMethodListeners(classLevelListeners, multiMethods, bean, beanName);
			}
		}
		return bean;
	}

	/*
	 * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
	 */
	private Collection<SubListener> findListenerAnnotations(Class<?> clazz) {
		Set<SubListener> listeners = new HashSet<SubListener>();
		SubListener ann = AnnotationUtils.findAnnotation(clazz, SubListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		SubListeners anns = AnnotationUtils.findAnnotation(clazz, SubListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.asList(anns.value()));
		}
		return listeners;
	}

	/*
	 * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
	 */
	private Set<SubListener> findListenerAnnotations(Method method) {
		Set<SubListener> listeners = new HashSet<SubListener>();
		SubListener ann = AnnotationUtils.findAnnotation(method, SubListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		SubListeners anns = AnnotationUtils.findAnnotation(method, SubListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.asList(anns.value()));
		}
		return listeners;
	}

	private void processMultiMethodListeners(Collection<SubListener> classLevelListeners, List<Method> multiMethods,
			Object bean, String beanName) {
		List<Method> checkedMethods = new ArrayList<Method>();
		for (Method method : multiMethods) {
			checkedMethods.add(checkProxy(method, bean));
		}
		for (SubListener classLevelListener : classLevelListeners) {
			MultiMethodSubListenerEndpoint endpoint = new MultiMethodSubListenerEndpoint(checkedMethods,
					bean);
			endpoint.setBeanFactory(this.beanFactory);
			processListener(endpoint, classLevelListener, bean, bean.getClass(), beanName);
		}
	}

	protected void processSubListener(SubListener SubListener, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodSubListenerEndpoint endpoint = new MethodSubListenerEndpoint();
		endpoint.setMethod(methodToUse);
		endpoint.setBeanFactory(this.beanFactory);
		processListener(endpoint, SubListener, bean, methodToUse, beanName);
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @SubListener method on the target class for this JDK proxy ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					}
					catch (NoSuchMethodException noMethod) {
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@SubListener method '%s' found on bean target class '%s', " +
						"but not found in any interface(s) for bean JDK proxy. Either " +
						"pull the method up to an interface or switch to subclass (CGLIB) " +
						"proxies by setting proxy-target-class/proxyTargetClass " +
						"attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	protected void processListener(MethodSubListenerEndpoint endpoint, SubListener SubListener, Object bean,
								   Object adminTarget, String beanName) {
		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setId(getEndpointId(SubListener));
		endpoint.setTopicPartitions(resolveTopicPartitions(SubListener));
		endpoint.setTopics(resolveTopics(SubListener));
		endpoint.setTopicPattern(resolvePattern(SubListener));
		String group = SubListener.group();
		if (StringUtils.hasText(group)) {
			Object resolvedGroup = resolveExpression(group);
			if (resolvedGroup instanceof String) {
				endpoint.setGroup((String) resolvedGroup);
			}
		}

		SubListenerContainerFactory<?> factory = null;
		String containerFactoryBeanName = resolve(SubListener.containerFactory());
		if (StringUtils.hasText(containerFactoryBeanName)) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
			try {
				factory = this.beanFactory.getBean(containerFactoryBeanName, SubListenerContainerFactory.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException("Could not register Sub listener endpoint on [" + adminTarget
						+ "] for bean " + beanName + ", no " + SubListenerContainerFactory.class.getSimpleName()
						+ " with id '" + containerFactoryBeanName + "' was found in the application context", ex);
			}
		}

		this.registrar.registerEndpoint(endpoint, factory);
	}

	private String getEndpointId(SubListener SubListener) {
		if (StringUtils.hasText(SubListener.id())) {
			return resolve(SubListener.id());
		}
		else {
			return "org.springframework.Sub.SubListenerEndpointContainer#" + this.counter.getAndIncrement();
		}
	}

	private TopicPartitionInitialOffset[] resolveTopicPartitions(SubListener SubListener) {
		TopicPartition[] topicPartitions = SubListener.topicPartitions();
		List<TopicPartitionInitialOffset> result = new ArrayList<>();
		if (topicPartitions.length > 0) {
			for (TopicPartition topicPartition : topicPartitions) {
				result.addAll(resolveTopicPartitionsList(topicPartition));
			}
		}
		return result.toArray(new TopicPartitionInitialOffset[result.size()]);
	}

	private String[] resolveTopics(SubListener SubListener) {
		String[] topics = SubListener.topics();
		List<String> result = new ArrayList<>();
		if (topics.length > 0) {
			for (int i = 0; i < topics.length; i++) {
				Object topic = resolveExpression(topics[i]);
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[result.size()]);
	}

	private Pattern resolvePattern(SubListener SubListener) {
		Pattern pattern = null;
		String text = SubListener.topicPattern();
		if (StringUtils.hasText(text)) {
			Object resolved = resolveExpression(text);
			if (resolved instanceof Pattern) {
				pattern = (Pattern) resolved;
			}
			else if (resolved instanceof String) {
				pattern = Pattern.compile((String) resolved);
			}
			else {
				throw new IllegalStateException(
						"topicPattern must resolve to a Pattern or String, not " + resolved.getClass());
			}
		}
		return pattern;
	}

	private List<TopicPartitionInitialOffset> resolveTopicPartitionsList(TopicPartition topicPartition) {
		Object topic = resolveExpression(topicPartition.topic());
		Assert.state(topic instanceof String,
				"topic in @TopicPartition must resolve to a String, not " + topic.getClass());
		Assert.state(StringUtils.hasText((String) topic), "topic in @TopicPartition must not be empty");
		String[] partitions = topicPartition.partitions();
		PartitionOffset[] partitionOffsets = topicPartition.partitionOffsets();
		Assert.state(partitions.length > 0 || partitionOffsets.length > 0,
				"At least one 'partition' or 'partitionOffset' required in @TopicPartition for topic '" + topic + "'");
		List<TopicPartitionInitialOffset> result = new ArrayList<>();
		for (int i = 0; i < partitions.length; i++) {
			resolvePartitionAsInteger((String) topic, resolveExpression(partitions[i]), result);
		}

		for (PartitionOffset partitionOffset : partitionOffsets) {
			Object partitionValue = resolveExpression(partitionOffset.partition());
			Integer partition;
			if (partitionValue instanceof String) {
				Assert.state(StringUtils.hasText((String) partitionValue),
						"partition in @PartitionOffset for topic '" + topic + "' cannot be empty");
				partition = Integer.valueOf((String) partitionValue);
			}
			else if (partitionValue instanceof Integer) {
				partition = (Integer) partitionValue;
			}
			else {
				throw new IllegalArgumentException(String.format(
						"@PartitionOffset for topic '%s' can't resolve '%s' as an Integer or String, resolved to '%s'",
							topic, partitionOffset.partition(), partitionValue.getClass()));
			}

			Object initialOffsetValue = resolveExpression(partitionOffset.initialOffset());
			Long initialOffset;
			if (initialOffsetValue instanceof String) {
				Assert.state(StringUtils.hasText((String) initialOffsetValue),
						"'initialOffset' in @PartitionOffset for topic '" + topic + "' cannot be empty");
				initialOffset = Long.valueOf((String) initialOffsetValue);
			}
			else if (initialOffsetValue instanceof Long) {
				initialOffset = (Long) initialOffsetValue;
			}
			else {
				throw new IllegalArgumentException(String.format(
						"@PartitionOffset for topic '%s' can't resolve '%s' as a Long or String, resolved to '%s'",
							topic, partitionOffset.initialOffset(), initialOffsetValue.getClass()));
			}

			Object relativeToCurrentValue = resolveExpression(partitionOffset.relativeToCurrent());
			Boolean relativeToCurrent;
			if (relativeToCurrentValue instanceof String) {
				relativeToCurrent = Boolean.valueOf((String) relativeToCurrentValue);
			}
			else if (relativeToCurrentValue instanceof Boolean) {
				relativeToCurrent = (Boolean) relativeToCurrentValue;
			}
			else {
				throw new IllegalArgumentException(String.format(
						"@PartitionOffset for topic '%s' can't resolve '%s' as a Boolean or String, resolved to '%s'",
							topic, partitionOffset.relativeToCurrent(), relativeToCurrentValue.getClass()));
			}

			TopicPartitionInitialOffset topicPartitionOffset =
					new TopicPartitionInitialOffset((String) topic, partition, initialOffset, relativeToCurrent);
			if (!result.contains(topicPartitionOffset)) {
				result.add(topicPartitionOffset);
			}
			else {
				throw new IllegalArgumentException(
						String.format("@TopicPartition can't have the same partition configuration twice: [%s]",
								topicPartitionOffset));
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		if (resolvedValue instanceof String) {
			result.add((String) resolvedValue);
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@SubListener can't resolve '%s' as a String", resolvedValue));
		}
	}

	@SuppressWarnings("unchecked")
	private void resolvePartitionAsInteger(String topic, Object resolvedValue,
			List<TopicPartitionInitialOffset> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolvePartitionAsInteger(topic, object, result);
			}
		}
		else if (resolvedValue instanceof String) {
			Assert.state(StringUtils.hasText((String) resolvedValue),
					"partition in @TopicPartition for topic '" + topic + "' cannot be empty");
			result.add(new TopicPartitionInitialOffset(topic, Integer.valueOf((String) resolvedValue)));
		}
		else if (resolvedValue instanceof Integer[]) {
			for (Integer partition : (Integer[]) resolvedValue) {
				result.add(new TopicPartitionInitialOffset(topic, partition));
			}
		}
		else if (resolvedValue instanceof Integer) {
			result.add(new TopicPartitionInitialOffset(topic, (Integer) resolvedValue));
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolvePartitionAsInteger(topic, object, result);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@SubListener for topic '%s' can't resolve '%s' as an Integer or String", topic, resolvedValue));
		}
	}

	private Object resolveExpression(String value) {
		String resolvedValue = resolve(value);

		if (!(resolvedValue.startsWith("#{") && value.endsWith("}"))) {
			return resolvedValue;
		}

		return this.resolver.evaluate(resolvedValue, this.expressionContext);
	}

	/**
	 * Resolve the specified value if possible.
	 * @param value the value to resolve
	 * @return the resolved value
	 * @see ConfigurableBeanFactory#resolveEmbeddedValue
	 */
	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	/**
	 * An {@link MessageHandlerMethodFactory} adapter that offers a configurable underlying
	 * instance to use. Useful if the factory to use is determined once the endpoints
	 * have been registered but not created yet.
	 * @see SubListenerEndpointRegistrar#setMessageHandlerMethodFactory
	 */
        private class SubHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		private MessageHandlerMethodFactory messageHandlerMethodFactory;

		public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory SubHandlerMethodFactory1) {
			this.messageHandlerMethodFactory = SubHandlerMethodFactory1;
		}

		@Override
		public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
			return getMessageHandlerMethodFactory().createInvocableHandlerMethod(bean, method);
		}

		private MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
			if (this.messageHandlerMethodFactory == null) {
				this.messageHandlerMethodFactory = createDefaultMessageHandlerMethodFactory();
			}
			return this.messageHandlerMethodFactory;
		}

		private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
            DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
            defaultFactory.setBeanFactory(SubListenerAnnotationBeanPostProcessor.this.beanFactory);
            defaultFactory.afterPropertiesSet();
            return defaultFactory;
        }

	}

}
