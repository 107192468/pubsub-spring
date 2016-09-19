/*
 * Copyright 2016 the original author or authors.
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

package com.wanda.ffan.sub.listener;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.wanda.ffan.common.TopicPartition;
import com.wanda.ffan.common.TopicPartitionInitialOffset;
import com.wanda.ffan.consumer.Consumer;
import com.wanda.ffan.consumer.ConsumerFactory;
import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.consumer.ConsumerRecords;
import com.wanda.ffan.sub.listener.event.ListenerContainerIdleEvent;
import com.wanda.ffan.exception.PubSubException;
import com.wanda.ffan.exception.WakeupException;
import com.wanda.ffan.sub.listener.config.ContainerProperties;

/**
 * Single-threaded Message listener container using the Java {@link Consumer} supporting
 * auto-partition assignment or user-configured assignment.
 * <p>
 * With the latter, initial partition offsets can be provided.
 *
 *
 * @author zhangling
 */
public class SubMessageListenerContainer extends AbstractMessageListenerContainer {

	private final ConsumerFactory consumerFactory;

	private final TopicPartitionInitialOffset[] topicPartitions;

	private ListenerConsumer listenerConsumer;

	private ListenableFuture<?> listenerConsumerFuture;

	private MessageListener listener;
	private AcknowledgingMessageListener acknowledgingMessageListener;

	/**
	 * Construct an instance with the supplied configuration properties.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	public SubMessageListenerContainer(ConsumerFactory consumerFactory,
			ContainerProperties containerProperties) {
		this(consumerFactory, containerProperties, (TopicPartitionInitialOffset[]) null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions/initialOffsets.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	public SubMessageListenerContainer(ConsumerFactory consumerFactory,
			ContainerProperties containerProperties, TopicPartitionInitialOffset... topicPartitions) {
		super(containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.consumerFactory = consumerFactory;
		if (topicPartitions != null) {
			this.topicPartitions = Arrays.copyOf(topicPartitions, topicPartitions.length);
		}
		else {
			this.topicPartitions = containerProperties.getTopicPartitions();
		}
	}

	/**
	 * Return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by pubsub; may be null if not assigned yet.
	 * @return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by pubsub; may be null if not assigned yet.
	 */
	public Collection<TopicPartition> getAssignedPartitions() {
		//TODO
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doStart() {
		if (isRunning()) {
			return;
		}
		ContainerProperties containerProperties = getContainerProperties();

		if (!this.consumerFactory.isAutoCommit()) {
			AckMode ackMode = containerProperties.getAckMode();
			if (ackMode.equals(AckMode.COUNT) || ackMode.equals(AckMode.COUNT_TIME)) {
				Assert.state(containerProperties.getAckCount() > 0, "'ackCount' must be > 0");
			}
			if ((ackMode.equals(AckMode.TIME) || ackMode.equals(AckMode.COUNT_TIME))
					&& containerProperties.getAckTime() == 0) {
				containerProperties.setAckTime(5000);
			}
		}

		Object messageListener = containerProperties.getMessageListener();
		Assert.state(messageListener != null, "A MessageListener is required");
		if (messageListener instanceof AcknowledgingMessageListener) {
			this.acknowledgingMessageListener = (AcknowledgingMessageListener) messageListener;
		}
		else if (messageListener instanceof MessageListener) {
			this.listener = (MessageListener) messageListener;
		}
		else {
			throw new IllegalStateException("messageListener must be 'MessageListener' "
					+ "or 'AcknowledgingMessageListener', not " + messageListener.getClass().getName());
		}
		if (containerProperties.getConsumerTaskExecutor() == null) {
			SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-pubsub-consumer-");
			containerProperties.setConsumerTaskExecutor(consumerExecutor);
		}
		if (containerProperties.getListenerTaskExecutor() == null) {
			SimpleAsyncTaskExecutor listenerExecutor = new SimpleAsyncTaskExecutor(
					(getBeanName() == null ? "" : getBeanName()) + "-pubsub-listener-");
			containerProperties.setListenerTaskExecutor(listenerExecutor);
		}
		this.listenerConsumer = new ListenerConsumer(this.listener, this.acknowledgingMessageListener);
		setRunning(true);
		this.listenerConsumerFuture = containerProperties
				.getConsumerTaskExecutor()
				.submitListenable(this.listenerConsumer);
	}

	@Override
	protected void doStop(final Runnable callback) {
		if (isRunning()) {
			this.listenerConsumerFuture.addCallback(new ListenableFutureCallback<Object>() {
				@Override
				public void onFailure(Throwable e) {
//					PubsubMessageListenerContainer.this.logger.error("Error while stopping the container: ", e);
					if (callback != null) {
						callback.run();
					}
				}

				@Override
				public void onSuccess(Object result) {
//					if (pubsubMessageListenerContainer.this.logger.isDebugEnabled()) {
//						pubsubMessageListenerContainer.this.logger
//								.debug(pubsubMessageListenerContainer.this + " stopped normally");
//					}
					if (callback != null) {
						callback.run();
					}
				}
			});
			setRunning(false);
			this.listenerConsumer.consumer.wakeup();
		}
	}


	@Override
	public String toString() {
		return "pubsubMessageListenerContainer [id=" + getBeanName() + ", topicPartitions=" + getAssignedPartitions()
				+ "]";
	}


	private final class ListenerConsumer implements SchedulingAwareRunnable {

		private final Log logger = LogFactory.getLog(ListenerConsumer.class);

		private final ContainerProperties containerProperties = getContainerProperties();


		private final Consumer consumer;

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final MessageListener listener;

		private final AcknowledgingMessageListener acknowledgingMessageListener;

		private final boolean autoCommit = SubMessageListenerContainer.this.consumerFactory.isAutoCommit();

		private final boolean isManualAck = this.containerProperties.getAckMode().equals(AckMode.MANUAL);

		private final boolean isManualImmediateAck =
				this.containerProperties.getAckMode().equals(AckMode.MANUAL_IMMEDIATE);

		private final boolean isAnyManualAck = this.isManualAck || this.isManualImmediateAck;

		private final boolean isRecordAck = this.containerProperties.getAckMode().equals(AckMode.RECORD);

		private final boolean isBatchAck = this.containerProperties.getAckMode().equals(AckMode.BATCH);

		private final BlockingQueue<ConsumerRecords> recordsToProcess =
				new LinkedBlockingQueue<>(this.containerProperties.getQueueDepth());

		private final BlockingQueue<ConsumerRecord> acks = new LinkedBlockingQueue<>();

		private final ApplicationEventPublisher applicationEventPublisher = getApplicationEventPublisher();

		private volatile Map<TopicPartition, OffsetMetadata> definedPartitions;

		private ConsumerRecords unsent;

		private volatile Collection<TopicPartition> assignedPartitions;

		private int count;

		private volatile ListenerInvoker invoker;

		private long last;

		private volatile Future<?> listenerInvokerFuture;

		/**
		 * The consumer is currently paused due to a slow listener. The consumer will be
		 * resumed when the current batch of records has been processed but will continue
		 * to be polled.
		 */
		private boolean paused;

		private ListenerConsumer(MessageListener listener, AcknowledgingMessageListener ackListener) {
			Assert.state(!this.isAnyManualAck || !this.autoCommit,
				"Consumer cannot be configured for auto commit for ackMode " + this.containerProperties.getAckMode());
			final Consumer consumer = SubMessageListenerContainer.this.consumerFactory.createConsumer();
			this.consumer = consumer;
			this.listener = listener;
			this.acknowledgingMessageListener = ackListener;
		}

		private void startInvoker() {
			ListenerConsumer.this.invoker = new ListenerInvoker();
			ListenerConsumer.this.listenerInvokerFuture = this.containerProperties.getListenerTaskExecutor()
					.submit(ListenerConsumer.this.invoker);
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			this.count = 0;
			this.last = System.currentTimeMillis();
			if (isRunning() && this.definedPartitions != null) {
				initPartitionsIfNeeded();
				// we start the invoker here as there will be no rebalance calls to
				// trigger it, but only if the container is not set to autocommit
				// otherwise we will process records on a separate thread
				if (!this.autoCommit) {
					startInvoker();
				}
			}
			long lastReceive = System.currentTimeMillis();
			long lastAlertAt = lastReceive;
			while (isRunning()) {
				try {
					if (!this.autoCommit) {
						processCommits();
					}
					if (this.logger.isTraceEnabled()) {
						this.logger.trace("Polling (paused=" + this.paused + ")...");
					}
					ConsumerRecords records = this.consumer.poll(this.containerProperties.getPollTimeout());
					if (records != null && this.logger.isDebugEnabled()) {
						this.logger.debug("Received: " + records.count() + " records");
					}
					if (records != null && records.count() > 0) {
						if (this.containerProperties.getIdleEventInterval() != null) {
							lastReceive = System.currentTimeMillis();
						}
						// if the container is set to auto-commit, then execute in the
						// same thread
						// otherwise send to the buffering queue
						if (this.autoCommit) {
							invokeListener(records);
						}
						else {
							if (sendToListener(records)) {
								if (this.assignedPartitions != null) {
									this.consumer.pause(this.assignedPartitions);
									this.paused = true;
									this.unsent = records;
								}
							}
						}
					}
					else {
						if (this.containerProperties.getIdleEventInterval() != null) {
							long now = System.currentTimeMillis();
							if (now > lastReceive + this.containerProperties.getIdleEventInterval()
									&& now > lastAlertAt + this.containerProperties.getIdleEventInterval()) {
								publishIdleContainerEvent(now - lastReceive);
								lastAlertAt = now;
							}
						}
					}
					this.unsent = checkPause(this.unsent);
				}
				catch (WakeupException e) {
					this.unsent = checkPause(this.unsent);
				}
				catch (Exception e) {
					if (this.containerProperties.getErrorHandler() != null) {
						this.containerProperties.getErrorHandler().handle(e, null);
					}
					else {
						this.logger.error("Container exception", e);
					}
				}
			}
			if (this.listenerInvokerFuture != null) {
				stopInvokerAndCommitManualAcks();
			}
			this.consumer.close();
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Consumer stopped");
			}
		}

		private void publishIdleContainerEvent(long idleTime) {
			if (this.applicationEventPublisher != null) {
				this.applicationEventPublisher.publishEvent(new ListenerContainerIdleEvent(
						SubMessageListenerContainer.this, idleTime, getBeanName(), getAssignedPartitions()));
			}
		}

		private void stopInvokerAndCommitManualAcks() {
			long now = System.currentTimeMillis();
			this.invoker.stop();
			long remaining = this.containerProperties.getShutdownTimeout() + now - System.currentTimeMillis();
			try {
				this.listenerInvokerFuture.get(remaining, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (ExecutionException e) {
				this.logger.error("Error while shutting down the listener invoker:", e);
			}
			catch (TimeoutException e) {
				this.logger.info("Invoker timed out while waiting for shutdown and will be canceled.");
				this.listenerInvokerFuture.cancel(true);
			}
			finally {
				this.listenerInvokerFuture = null;
			}
			processCommits();
			if (this.offsets.size() > 0) {
				// we always commit after stopping the invoker
			}
			this.invoker = null;
		}

		private ConsumerRecords checkPause(ConsumerRecords unsent) {
			//TODO
			return unsent;
		}

		private boolean sendToListener(final ConsumerRecords records) throws InterruptedException {
			if (this.containerProperties.isPauseEnabled() && CollectionUtils.isEmpty(this.definedPartitions)) {
				return !this.recordsToProcess.offer(records, this.containerProperties.getPauseAfter(),
						TimeUnit.MILLISECONDS);
			}
			else {
				this.recordsToProcess.put(records);
				return false;
			}
		}

		/**
		 * Process any acks that have been queued by the listener thread.
		 */
		private void handleAcks() {
			ConsumerRecord record = this.acks.poll();
			while (record != null) {
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Ack: " + record);
				}
				processAck(record);
				record = this.acks.poll();
			}
		}

		private void processAck(ConsumerRecord record) {
			if (ListenerConsumer.this.isManualImmediateAck) {
				try {
					ackImmediate(record);
				}
				catch (WakeupException e) {
					// ignore - not polling
				}
			}
			else {
				addOffset(record);
			}
		}

		private void ackImmediate(ConsumerRecord record) {
		
		}

		private void invokeListener(final ConsumerRecords records) {
			Iterator<ConsumerRecord> iterator = records.iterator();
			while (iterator.hasNext() && (this.autoCommit || (this.invoker != null && this.invoker.active))) {
				final ConsumerRecord record = iterator.next();
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("Processing " + record);
				}
				try {
					if (this.acknowledgingMessageListener != null) {
						this.acknowledgingMessageListener.onMessage(record,
								new ConsumerAcknowledgment(record, this.isManualImmediateAck));
					}
					else {
						this.listener.onMessage(record);
					}
					if (!this.isAnyManualAck && !this.autoCommit) {
						this.acks.add(record);
					}
					if (this.isRecordAck) {
						this.consumer.wakeup();
					}
				}
				catch (Exception e) {
					if (this.containerProperties.isAckOnError() && !this.autoCommit) {
						this.acks.add(record);
					}
					if (this.containerProperties.getErrorHandler() != null) {
						this.containerProperties.getErrorHandler().handle(e, record);
					}
					else {
						this.logger.error("Listener threw an exception and no error handler for " + record, e);
					}
				}
			}
			if (this.isManualAck || this.isBatchAck) {
				this.consumer.wakeup();
			}
		}

		private void processCommits() {
			handleAcks();
			this.count += this.acks.size();
			long now;
			AckMode ackMode = this.containerProperties.getAckMode();
			if (!this.isManualImmediateAck) {
				if (!this.isManualAck) {
					updatePendingOffsets();
				}
				boolean countExceeded = this.count >= this.containerProperties.getAckCount();
				if (this.isManualAck || this.isBatchAck || this.isRecordAck
						|| (ackMode.equals(AckMode.COUNT) && countExceeded)) {
					if (this.logger.isDebugEnabled() && ackMode.equals(AckMode.COUNT)) {
						this.logger.debug("Committing in AckMode.COUNT because count " + this.count
								+ " exceeds configured limit of " + this.containerProperties.getAckCount());
					}
					this.count = 0;
				}
				else {
					now = System.currentTimeMillis();
					boolean elapsed = now - this.last > this.containerProperties.getAckTime();
					if (ackMode.equals(AckMode.TIME) && elapsed) {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Committing in AckMode.TIME " +
											"because time elapsed exceeds configured limit of " +
											this.containerProperties.getAckTime());
						}
						this.last = now;
					}
					else if (ackMode.equals(AckMode.COUNT_TIME) && (elapsed || countExceeded)) {
						if (this.logger.isDebugEnabled()) {
							if (elapsed) {
								this.logger.debug("Committing in AckMode.COUNT_TIME " +
										"because time elapsed exceeds configured limit of " +
										this.containerProperties.getAckTime());
							}
							else {
								this.logger.debug("Committing in AckMode.COUNT_TIME " +
										"because count " + this.count + " exceeds configured limit of" +
										this.containerProperties.getAckCount());
							}
						}

						this.last = now;
						this.count = 0;
					}
				}
			}
		}

		private void initPartitionsIfNeeded() {
			/*
			 * Note: initial position setting is only supported with explicit topic assignment.
			 * When using auto assignment (subscribe), the ConsumerRebalanceListener is not
			 * called until we poll() the consumer.
			 */
		}

		private void updatePendingOffsets() {
			ConsumerRecord record = this.acks.poll();
			while (record != null) {
				addOffset(record);
				record = this.acks.poll();
			}
		}

		private void addOffset(ConsumerRecord record) {
			if (!this.offsets.containsKey(record.topic())) {
				this.offsets.put(record.topic(), new HashMap<Integer, Long>());
			}
			this.offsets.get(record.topic()).put(record.partition(), record.offset());
		}

		

		private final class ListenerInvoker implements SchedulingAwareRunnable {

			private final CountDownLatch exitLatch = new CountDownLatch(1);

			private volatile boolean active = true;

			private volatile Thread executingThread;

			@Override
			public void run() {
				Assert.isTrue(this.active, "This instance is not active anymore");
				try {
					this.executingThread = Thread.currentThread();
					while (this.active) {
						try {
							ConsumerRecords records = ListenerConsumer.this.recordsToProcess.poll(1,
									TimeUnit.SECONDS);
							if (this.active) {
								if (records != null) {
									invokeListener(records);
								}
								else {
									if (ListenerConsumer.this.logger.isTraceEnabled()) {
										ListenerConsumer.this.logger.trace("No records to process");
									}
								}
							}
						}
						catch (InterruptedException e) {
							if (!this.active) {
								Thread.currentThread().interrupt();
							}
							else {
								ListenerConsumer.this.logger.debug("Interrupt ignored");
							}
						}
						if (!ListenerConsumer.this.isManualImmediateAck && this.active) {
							ListenerConsumer.this.consumer.wakeup();
						}
					}
				}
				finally {
					this.active = false;
					this.exitLatch.countDown();
				}
			}

			@Override
			public boolean isLongLived() {
				return true;
			}

			private void stop() {
				if (ListenerConsumer.this.logger.isDebugEnabled()) {
					ListenerConsumer.this.logger.debug("Stopping invoker");
				}
				this.active = false;
				try {
					if (!this.exitLatch.await(getContainerProperties().getShutdownTimeout(), TimeUnit.MILLISECONDS)
							&& this.executingThread != null) {
						if (ListenerConsumer.this.logger.isDebugEnabled()) {
							ListenerConsumer.this.logger.debug("Interrupting invoker");
						}
						this.executingThread.interrupt();
					}
				}
				catch (InterruptedException e) {
					if (this.executingThread != null) {
						this.executingThread.interrupt();
					}
					Thread.currentThread().interrupt();
				}
				if (ListenerConsumer.this.logger.isDebugEnabled()) {
					ListenerConsumer.this.logger.debug("Invoker stopped");
				}
			}
		}

		private final class ConsumerAcknowledgment implements Acknowledgment {

			private final ConsumerRecord record;

			private final boolean immediate;

			private ConsumerAcknowledgment(ConsumerRecord record, boolean immediate) {
				this.record = record;
				this.immediate = immediate;
			}

			@Override
			public void acknowledge() {
				try {
					if (ListenerConsumer.this.autoCommit) {
						throw new IllegalStateException("Manual acks are not allowed when auto commit is used");
					}
					ListenerConsumer.this.acks.put(this.record);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new PubSubException("Interrupted while queuing ack for " + this.record, e);
				}
				if (this.immediate) {
					ListenerConsumer.this.consumer.wakeup();
				}
			}

			@Override
			public String toString() {
				return "Acknowledgment for " + this.record;
			}

		}

	}


	private static final class OffsetMetadata {

		private final Long offset;

		private final boolean relativeToCurrent;

		private OffsetMetadata(Long offset, boolean relativeToCurrent) {
			this.offset = offset;
			this.relativeToCurrent = relativeToCurrent;
		}

	}



}
