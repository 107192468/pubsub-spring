
package com.wanda.ffan.sub.listener;

import com.wanda.ffan.consumer.ConsumerRecord;

/**
 * Listener for handling incoming Kafka messages, propagating an acknowledgment handle that recipients
 * can invoke when the message has been processed.
 *
 *
 * @author zhangling
 */
public interface AcknowledgingMessageListener{

	/**
	 * Executes when a Kafka message is received.
	 * @param record the Kafka message to be processed
	 * @param acknowledgment a handle for acknowledging the message processing
	 */
	void onMessage(ConsumerRecord record, Acknowledgment acknowledgment);

}
