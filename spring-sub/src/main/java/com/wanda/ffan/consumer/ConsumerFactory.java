package com.wanda.ffan.consumer;


/**
 * The strategy to produce a {@link Consumer} instance(s).
 *
 * @author zhangling
 */
public interface ConsumerFactory {
	Consumer createConsumer();

	boolean isAutoCommit();
}
