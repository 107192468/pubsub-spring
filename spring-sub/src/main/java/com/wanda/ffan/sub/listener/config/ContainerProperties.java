package com.wanda.ffan.sub.listener.config;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.util.Assert;

import com.wanda.ffan.common.TopicPartitionInitialOffset;
import com.wanda.ffan.exception.ErrorHandler;
import com.wanda.ffan.exception.LoggingErrorHandler;
import com.wanda.ffan.sub.listener.AbstractMessageListenerContainer;
import com.wanda.ffan.sub.listener.AbstractMessageListenerContainer.AckMode;
import com.wanda.ffan.sub.listener.AcknowledgingMessageListener;
import com.wanda.ffan.sub.listener.MessageListener;

/**
 * Contains runtime properties for a listener container.
 *
 * @author zhangling
 */
public class ContainerProperties {

	private static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;

	private static final int DEFAULT_QUEUE_DEPTH = 1;
	private static final int DEFAULT_PAUSE_AFTER = 10000;

	/**
	 * Topic names.
	 */
	private final String[] topics;

	/**
	 * Topic pattern.
	 */
	private final Pattern topicPattern;
	/**
	 * Topics/partitions/initial offsets.
	 */
	private final TopicPartitionInitialOffset[] topicPartitions;
	
	private AbstractMessageListenerContainer.AckMode ackMode = AckMode.BATCH;

	/**
	 * The number of outstanding record count after which offsets should be
	 * committed when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being
	 * used.
	 */
	private int ackCount;

	/**
	 * The time (ms) after which outstanding offsets should be committed when
	 * {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should be
	 * larger than
	 */
	private long ackTime;

	/**
	 * The message listener; must be a {@link MessageListener} or
	 * {@link AcknowledgingMessageListener}.
	 */
	private Object messageListener;

	/**
	 * The max time to block in the consumer waiting for records.
	 */
	private volatile long pollTimeout = 1000;

	/**
	 * The executor for threads that poll the consumer.
	 */
	private AsyncListenableTaskExecutor consumerTaskExecutor;

	/**
	 * The executor for threads that invoke the listener.
	 */
	private AsyncListenableTaskExecutor listenerTaskExecutor;

	/**
	 * The error handler to call when the listener throws an exception.
	 */
	private ErrorHandler errorHandler = new LoggingErrorHandler();


	
	/**
	 * Set the queue depth for handoffs from the consumer thread to the listener
	 * thread. Default 1 (up to 2 in process).
	 */
	private int queueDepth = DEFAULT_QUEUE_DEPTH;

	/**
	 * The timeout for shutting down the container. This is the maximum amount of
	 * time that the invocation to {@code #stop(Runnable)} will block for, before
	 * returning.
	 */
	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;



	private boolean syncCommits = true;

	private boolean ackOnError = true;

	private Long idleEventInterval;

	/**
	 * When true, avoids rebalancing when this consumer is slow or throws a
	 * qualifying exception - pauses the consumer. Default: true.
	 * @see #pauseAfter
	 */
	private boolean pauseEnabled = true;
	
	private long pauseAfter = DEFAULT_PAUSE_AFTER;
	public ContainerProperties(String... topics) {
//		Assert.notEmpty(topics, "An array of topicPartitions must be provided");
		this.topics = Arrays.asList(topics).toArray(new String[topics.length]);
		this.topicPattern = null;
		this.topicPartitions = null;
	}

	public ContainerProperties(Pattern topicPattern) {
		this.topics = null;
		this.topicPattern = topicPattern;
		this.topicPartitions = null;
	}

	public ContainerProperties(TopicPartitionInitialOffset... topicPartitions) {
		this.topics = null;
		this.topicPattern = null;
//		Assert.notEmpty(topicPartitions, "An array of topicPartitions must be provided");
		if(topicPartitions==null){
			this.topicPartitions=null;
		}
		else {
			this.topicPartitions = new LinkedHashSet<>(Arrays.asList(topicPartitions))
				.toArray(new TopicPartitionInitialOffset[topicPartitions.length]);
		}
	}


	/**
	 * Set the message listener; must be a {@link MessageListener} or
	 * {@link AcknowledgingMessageListener}.
	 * @param messageListener the listener.
	 */
	public void setMessageListener(Object messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * Set the ack mode to use when auto ack (in the configuration properties) is false.
	 * <ul>
	 * <li>RECORD: Ack after each record has been passed to the listener.</li>
	 * <li>BATCH: Ack after each batch of records received from the consumer has been
	 * passed to the listener</li>
	 * <li>TIME: Ack after this number of milliseconds; (should be greater than
	 * {@code #setPollTimeout(long) pollTimeout}.</li>
	 * <li>COUNT: Ack after at least this number of records have been received</li>
	 * <li>MANUAL: Listener is responsible for acking - use a
	 * {@link AcknowledgingMessageListener}.
	 * </ul>
	 * @param ackMode the {@link AckMode}; default BATCH.
	 */
	public void setAckMode(AbstractMessageListenerContainer.AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * Set the max time to block in the consumer waiting for records.
	 * @param pollTimeout the timeout in ms; default 1000.
	 */
	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * Set the number of outstanding record count after which offsets should be
	 * committed when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being used.
	 * @param count the count
	 */
	public void setAckCount(int count) {
		Assert.state(count > 0, "'ackCount' must be > 0");
		this.ackCount = count;
	}

	/**
	 * Set the time (ms) after which outstanding offsets should be committed when
	 * {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should be
	 * larger than
	 * @param ackTime the time
	 */
	public void setAckTime(long ackTime) {
		Assert.state(ackTime > 0, "'ackTime' must be > 0");
		this.ackTime = ackTime;
	}
	
	
	/**
	 * Set the error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the executor for threads that poll the consumer.
	 * @param consumerTaskExecutor the executor
	 */
	public void setConsumerTaskExecutor(AsyncListenableTaskExecutor consumerTaskExecutor) {
		this.consumerTaskExecutor = consumerTaskExecutor;
	}

	/**
	 * Set the executor for threads that invoke the listener.
	 * @param listenerTaskExecutor the executor.
	 */
	public void setListenerTaskExecutor(AsyncListenableTaskExecutor listenerTaskExecutor) {
		this.listenerTaskExecutor = listenerTaskExecutor;
	}



	/**
	 * Set the queue depth for handoffs from the consumer thread to the listener
	 * thread. Default 1 (up to 2 in process).
	 * @param queueDepth the queue depth.
	 */
	public void setQueueDepth(int queueDepth) {
		this.queueDepth = queueDepth;
	}

	/**
	 * Set the timeout for shutting down the container. This is the maximum amount of
	 * time that the invocation to {@code #stop(Runnable)} will block for, before
	 * returning.
	 * @param shutdownTimeout the shutdown timeout.
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	
	public void setSyncCommits(boolean syncCommits) {
		this.syncCommits = syncCommits;
	}

	public void setIdleEventInterval(Long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	public void setAckOnError(boolean ackOnError) {
		this.ackOnError = ackOnError;
	}

	public String[] getTopics() {
		return this.topics;
	}

	public Pattern getTopicPattern() {
		return this.topicPattern;
	}


	public AbstractMessageListenerContainer.AckMode getAckMode() {
		return this.ackMode;
	}

	public int getAckCount() {
		return this.ackCount;
	}

	public long getAckTime() {
		return this.ackTime;
	}

	public Object getMessageListener() {
		return this.messageListener;
	}

	public long getPollTimeout() {
		return this.pollTimeout;
	}

	public AsyncListenableTaskExecutor getConsumerTaskExecutor() {
		return this.consumerTaskExecutor;
	}

	public AsyncListenableTaskExecutor getListenerTaskExecutor() {
		return this.listenerTaskExecutor;
	}

	public ErrorHandler getErrorHandler() {
		return this.errorHandler;
	}


	public int getQueueDepth() {
		return this.queueDepth;
	}

	public long getShutdownTimeout() {
		return this.shutdownTimeout;
	}


	public boolean isSyncCommits() {
		return this.syncCommits;
	}

	public Long getIdleEventInterval() {
		return this.idleEventInterval;
	}

	public boolean isAckOnError() {
		return this.ackOnError;
	}
	public TopicPartitionInitialOffset[] getTopicPartitions() {
		return this.topicPartitions;
	}
	
	public void setPauseAfter(long pauseAfter) {
		this.pauseAfter = pauseAfter;
	}

	/**
	 * Set to true to avoid rebalancing when this consumer is slow or throws a
	 * qualifying exception - pause the consumer. Default: true.
	 * @param pauseEnabled true to pause.
	 * @see #setPauseAfter(long)
	 */
	public void setPauseEnabled(boolean pauseEnabled) {
		this.pauseEnabled = pauseEnabled;
	}
	public boolean isPauseEnabled() {
		return this.pauseEnabled;
	}

	public long getPauseAfter() {
		return pauseAfter;
	}
	
	
}
