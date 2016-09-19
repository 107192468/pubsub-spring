package com.wanda.ffan.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.context.ApplicationEvent;

import com.wanda.ffan.common.TopicPartition;

public class ListenerContainerIdleEvent extends ApplicationEvent{
	private static final long serialVersionUID = 7099057708183571909L;
	private final long idleTime;

	private final String listenerId;

	private final List<TopicPartition> topicPartitions;

	public ListenerContainerIdleEvent(Object source, long idleTime, String id,
			Collection<TopicPartition> topicPartitions) {
		super(source);
		this.idleTime = idleTime;
		this.listenerId = id;
		this.topicPartitions = new ArrayList<>(topicPartitions);
	}

	/**
	 * How long the container has been idle.
	 * @return the time in milliseconds.
	 */
	public long getIdleTime() {
		return this.idleTime;
	}

	/**
	 * The TopicPartitions the container is listening to.
	 * @return the TopicPartition list.
	 */
	public Collection<TopicPartition> getTopicPartitions() {
		return Collections.unmodifiableList(this.topicPartitions);
	}

	/**
	 * The id of the listener (if {@code @RabbitListener}) or the container bean name.
	 * @return the id.
	 */
	public String getListenerId() {
		return this.listenerId;
	}

	@Override
	public String toString() {
		return "ListenerContainerIdleEvent [idleTime="
				+ ((float) this.idleTime / 1000) + "s, listenerId=" + this.listenerId
				+ ", container=" + getSource()
				+ ", topicPartitions=" + this.topicPartitions + "]";
	}
}
