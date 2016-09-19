package com.wanda.ffan.exception;

import com.wanda.ffan.consumer.ConsumerRecord;

/**
 * @author zhangling
 */
public interface ErrorHandler {
	void handle(Exception thrownException, ConsumerRecord record);
}
