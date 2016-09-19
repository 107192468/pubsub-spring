package com.wanda.ffan.consumer;


public final class ConsumerRecord {

	    private final String topic;
	    private final int partition;
	    private final long offset;
	    private final long timestamp;
	    private final String massage;
		private final String key;
	    /**
	     * Creates a record to be received from a specified topic and partition (provided for
	     * compatibility with Kafka 0.9 before the message format supported timestamps and before
	     * serialized metadata were exposed).
	     *
	     * @param topic The topic this record is received from
	     * @param partition The partition of the topic this record is received from
	     * @param offset The offset of this record in the corresponding Kafka partition
	     */
	    public ConsumerRecord(String topic,
	                          int partition,
	                          long offset,
	                          String massage,String key,long timestamp) {
	        this.massage=massage;
	        this.partition=partition;
	        this.offset=offset;
	        this.topic=topic;
	        this.timestamp=timestamp;
			this.key=key;
	    }
	    public ConsumerRecord(String topic,
                int partition,
                long offset,
                String massage,String key) {
					this(topic,partition,offset,massage,key,System.currentTimeMillis());
	    }

	   

	    /**
	     * The topic this record is received from
	     */
	    public String topic() {
	        return this.topic;
	    }

	    /**
	     * The partition from which this record is received
	     */
	    public int partition() {
	        return this.partition;
	    }

	   

	    /**
	     * The position of this record in the corresponding Kafka partition.
	     */
	    public long offset() {
	        return offset;
	    }

	    /**
	     * The timestamp of this record
	     */
	    public long timestamp() {
	        return timestamp;
	    }

	    public String massage(){
	    	return massage;
	    }

		public String key() {
			return key;
		}

	@Override
	    public String toString() {
	        return "ConsumerRecord(topic = " + topic() + ", partition = " + partition() + ", offset = " + offset()
	             +"key:"+key() +"massage:"+massage() + "timestamp:"+timestamp() + ")";
	    }
}
