package com.wanda.ffan.sub.listener;

public interface Acknowledgment {
	/**
	 * Invoked when the message for which the acknowledgment has been created has been processed.
	 * Calling this method implies that all the previous messages in the partition have been processed already.
	 */
	void acknowledge();
}
