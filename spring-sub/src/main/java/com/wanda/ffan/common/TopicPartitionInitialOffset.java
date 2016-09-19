package com.wanda.ffan.common;

import java.util.Objects;


public class TopicPartitionInitialOffset {

	private final TopicPartition topicPartition;

	private final Long initialOffset;

	private final boolean relativeToCurrent;

	/**
	 * Construct an instance with no initial offset management.
	 * @param topic the topic.
	 * @param partition the partition.
	 */
	public TopicPartitionInitialOffset(String topic, int partition) {
		this(topic, partition, null);
	}

	/**
	 * Construct an instance with the provided initial offset with
	 * {@link #isRelativeToCurrent()} false.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param initialOffset the initial offset.
	 * @see #TopicPartitionInitialOffset(String, int, Long, boolean)
	 */
	public TopicPartitionInitialOffset(String topic, int partition, Long initialOffset) {
		this(topic, partition, initialOffset, false);
	}

	/**
	 * Construct an instance with the provided initial offset.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param initialOffset the initial offset.
	 * @param relativeToCurrent true for the initial offset to be relative to
	 * the current consumer position, false for a positive initial offset to
	 * be absolute and a negative offset relative to the current end of the
	 * partition.
	 * @since 1.1
	 */
	public TopicPartitionInitialOffset(String topic, int partition, Long initialOffset, boolean relativeToCurrent) {
		this.topicPartition = new TopicPartition(topic, partition);
		this.initialOffset = initialOffset;
		this.relativeToCurrent = relativeToCurrent;
	}

	public TopicPartition topicPartition() {
		return this.topicPartition;
	}

	public int partition() {
		return this.topicPartition.partition();
	}

	public String topic() {
		return this.topicPartition.topic();
	}

	public Long initialOffset() {
		return this.initialOffset;
	}

	public boolean isRelativeToCurrent() {
		return this.relativeToCurrent;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TopicPartitionInitialOffset that = (TopicPartitionInitialOffset) o;
		return Objects.equals(this.topicPartition, that.topicPartition);
	}

	@Override
	public int hashCode() {
		return this.topicPartition.hashCode();
	}

	@Override
	public String toString() {
		return "TopicPartitionInitialOffset{" +
				"topicPartition=" + this.topicPartition +
				", initialOffset=" + this.initialOffset +
				", relativeToCurrent=" + this.relativeToCurrent +
				'}';
	}
}
