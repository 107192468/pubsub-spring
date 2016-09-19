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
public @interface PartitionOffset {
	/**
	 * The partition within the topic to listen on.
	 * Property place holders and SpEL expressions are supported,
	 * which must resolve to Integer (or String that can be parsed as Integer).
	 * @return partition within the topic.
	 */
	String partition();

	/**
	 * The initial offset of the {@link #partition()}.
	 * Property place holders and SpEL expressions are supported,
	 * which must resolve to Long (or String that can be parsed as Long).
	 * @return initial offset.
	 */
	String initialOffset();

	/**
	 * By default, positive {@link #initialOffset()} is absolute, negative
	 * is relative to the current topic end. When this is 'true', the
	 * initial offset (positive or negative) is relative to the current
	 * consumer position.
	 * @return whether or not the offset is relative to the current position.
	 * @since 1.1
	 */
	String relativeToCurrent() default "false";
}
