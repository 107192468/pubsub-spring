package com.wanda.ffan.consumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.wanda.ffan.common.TopicPartition;
import com.wanda.ffan.common.utils.AbstractIterator;




public class ConsumerRecords implements Iterable<ConsumerRecord>{
	   @SuppressWarnings("unchecked")
	    public static final ConsumerRecords EMPTY = new ConsumerRecords (Collections.EMPTY_MAP);

	    private final Map<TopicPartition, List<ConsumerRecord>> records;

	    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord>> records) {
	        this.records = records;
	    }

	    
	    
	public Iterator<ConsumerRecord> iterator() {
		 return new ConcatenatedIterable(records.values()).iterator();
	}
	
	 public int count() {
	        int count = 0;
	        for (List<ConsumerRecord> recs: this.records.values())
	            count += recs.size();
	        return count;
	    }
	 
	 private static class ConcatenatedIterable implements Iterable<ConsumerRecord> {

	        private final Iterable<? extends Iterable<ConsumerRecord>> iterables;

	        public ConcatenatedIterable(Iterable<? extends Iterable<ConsumerRecord>> iterables) {
	            this.iterables = iterables;
	        }
	        @Override
	        public Iterator<ConsumerRecord> iterator() {
	            return new AbstractIterator<ConsumerRecord>() {
	                Iterator<? extends Iterable<ConsumerRecord>> iters = iterables.iterator();
	                Iterator<ConsumerRecord> current;

	                public ConsumerRecord makeNext() {
	                    while (current == null || !current.hasNext()) {
	                        if (iters.hasNext())
	                            current = iters.next().iterator();
	                        else
	                            return allDone();
	                    }
	                    return current.next();
	                }
	            };
	        }
	    }
	 
	 public boolean isEmpty() {
	        return records.isEmpty();
	    }
}
