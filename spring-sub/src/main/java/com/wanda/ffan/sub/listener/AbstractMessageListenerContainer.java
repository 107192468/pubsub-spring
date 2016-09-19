

package com.wanda.ffan.sub.listener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.wanda.ffan.sub.SubConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import com.wanda.ffan.sub.listener.config.ContainerProperties;

/**
 * The base implementation for the {@link MessageListenerContainer}.
 *
 * @author zhangling
 */
public abstract class AbstractMessageListenerContainer
		implements MessageListenerContainer, BeanNameAware, ApplicationEventPublisherAware, SmartLifecycle {
	protected  final Logger logger = LoggerFactory.getLogger(this.getClass());
	/**
	 * The offset commit behavior enumeration.
	 */
	public enum AckMode {

		/**
		 * Commit after each record is processed by the listener.
		 */
		RECORD,

		/**
		 * Commit whatever has already been processed before the next poll.
		 */
		BATCH,

		/**
		 * Commit pending updates after
		 * {@link ContainerProperties#setAckTime(long) ackTime} has elapsed.
		 */
		TIME,

		/**
		 * Commit pending updates after
		 * {@link ContainerProperties#setAckCount(int) ackCount} has been
		 * exceeded.
		 */
		COUNT,

		/**
		 * Commit pending updates after
		 * {@link ContainerProperties#setAckCount(int) ackCount} has been
		 * exceeded or after {@link ContainerProperties#setAckTime(long)
		 * ackTime} has elapsed.
		 */
		COUNT_TIME,

		/**
		 * User takes responsibility for acks using an
		 * {@link AcknowledgingMessageListener}.
		 */
		MANUAL,

		/**
		 * User takes responsibility for acks using an
		 * {@link AcknowledgingMessageListener}. The consumer is woken to
		 * immediately process the commit.
		 */
		MANUAL_IMMEDIATE,

	}

	private final ContainerProperties containerProperties;

	private final Object lifecycleMonitor = new Object();

	private String beanName;

	private ApplicationEventPublisher applicationEventPublisher;

	private boolean autoStartup = true;

	private int phase = 0;
	private volatile boolean running = false;

	protected AbstractMessageListenerContainer(ContainerProperties containerProperties) {
		Assert.notNull(containerProperties, "'containerProperties' cannot be null");
		if (containerProperties.getTopics() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopics());
		}else if (containerProperties.getTopicPattern() != null) {
			this.containerProperties = new ContainerProperties(containerProperties.getTopicPattern());
		}else {
			this.containerProperties = new ContainerProperties(containerProperties.getTopicPartitions());
		}
		BeanUtils.copyProperties(containerProperties, this.containerProperties,
				"topics", "topicPartitions", "topicPattern", "ackCount", "ackTime");
		if (containerProperties.getAckCount() > 0) {
			this.containerProperties.setAckCount(containerProperties.getAckCount());
		}
		if (containerProperties.getAckTime() > 0) {
			this.containerProperties.setAckTime(containerProperties.getAckTime());
		}
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	public ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	public boolean isRunning() {
		return this.running;
	}


	public ContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	public void setupMessageListener(Object messageListener) {
		this.containerProperties.setMessageListener(messageListener);
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			Assert.isTrue(
					this.containerProperties.getMessageListener() instanceof MessageListener|| this.containerProperties.getMessageListener() instanceof AcknowledgingMessageListener);
			doStart();
		}
	}

	protected abstract void doStart();

	@Override
	public final void stop() {
		final CountDownLatch latch = new CountDownLatch(1);
		stop(new Runnable() {
			@Override
			public void run() {
				latch.countDown();
			}
		});
		try {
			latch.await(this.containerProperties.getShutdownTimeout(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
		}
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (this.lifecycleMonitor) {
			doStop(callback);
		}
	}

	protected abstract void doStop(Runnable callback);

	@Override
	public int getPhase() {
		return this.phase;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}
	

}
