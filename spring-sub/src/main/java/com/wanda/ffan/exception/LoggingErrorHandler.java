package com.wanda.ffan.exception;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.ObjectUtils;

import com.wanda.ffan.consumer.ConsumerRecord;

public class LoggingErrorHandler implements ErrorHandler {

	private static final Log log = LogFactory.getLog(LoggingErrorHandler.class);

	@Override
	public void handle(Exception thrownException, ConsumerRecord record) {
		log.error("Error while processing: " + ObjectUtils.nullSafeToString(record), thrownException);
	}

}