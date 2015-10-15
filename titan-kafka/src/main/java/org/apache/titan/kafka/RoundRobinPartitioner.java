package org.apache.titan.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/*
 *
 */
public class RoundRobinPartitioner implements Partitioner
{
    private static Logger LOGGER = LoggerFactory.getLogger(RoundRobinPartitioner.class);
    private static AtomicInteger nextNum = new AtomicInteger();

    public RoundRobinPartitioner() {
    }

    public RoundRobinPartitioner(VerifiableProperties props) {
    }

    /**
     * Note that key is not even used for this basic round robin
     * partitioner.
     *
     * @param key - a key from the actual data - not used
     * @param numPartitions - number of partitions for the topic
     * @return partition to use
     */
    public int partition(Object key, int numPartitions)
    {
        int num = nextNum.getAndIncrement();
        if (num == Integer.MAX_VALUE || num < 0)
        {
            nextNum.set(0);
            return 0;
        }
        else
        {
            return (num % numPartitions);
        }
    }
}
