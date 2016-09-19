package com.wanda.ffan.consumer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Set;

import com.wanda.ffan.common.TopicPartition;




public interface Consumer  extends Closeable{

    /**
     */
    public Set<String> subscription();

    /**
     */
    public void subscribe(Collection<String> topics);

    /**
     */
    public ConsumerRecords poll(long timeout);



    /**
     */
    public void close();

    /**
     */
    public void wakeup();
    public void pause(Collection<TopicPartition> partitions);
}
