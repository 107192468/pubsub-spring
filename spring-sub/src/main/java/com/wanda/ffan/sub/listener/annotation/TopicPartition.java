package com.wanda.ffan.sub.listener.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @author zhangling
 *
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface TopicPartition {
	/**
	 * The topic to listen on.
	 * @return the topic to listen on. Property place holders
	 * and SpEL expressions are supported, which must resolve
	 * to a String.
	 */
	String topic();

	/**
	 * The partitions within the topic.
	 * Partitions specified here can't be duplicated in {@link #partitionOffsets()}.
	 * @return the partitions within the topic. Property place
	 * holders and SpEL expressions are supported, which must
	 * resolve to Integers (or Strings that can be parsed as
	 * Integers).
	 */
	String[] partitions() default {};

	/**
	 * The partitions with initial offsets within the topic.
	 * Partitions specified here can't be duplicated in the {@link #partitions()}.
	 * @return the {@link PartitionOffset} array.
	 */
	PartitionOffset[] partitionOffsets() default {};
}
