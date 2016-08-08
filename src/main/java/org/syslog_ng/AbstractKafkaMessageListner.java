package org.syslog_ng;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Properties;

/**
 * Abstract class of MessageListner
 */
public abstract class AbstractKafkaMessageListner {

    protected int threadCount;
    protected List<String> topics;
    protected ConsumerConnector consumerConnector;
    protected List<ConsumerIterator<byte[], byte[]>> consumerIteraror;

    /**
     * Initiate message listener
     *
     * @param properties properties for consumer
     * @param topic      list of topics to consume messages
     */
    public abstract void init(Properties properties, List<String> topic);

    /**
     * It will create consumer connector
     *
     * @param threadsCount number of threads to run
     * @return true if successfully created
     * @throws Exception
     */
    public abstract boolean createKafkaConnector(int threadsCount) throws Exception;

    /**
     * It will initiate consumers and start the thread pooling
     *
     * @param threadCount number of threads to be in thread pool
     * @throws Exception
     */
    public abstract void start(int threadCount) throws Exception;

    public abstract boolean hasNext();

    public abstract String readMessages(ConsumerIterator<byte[], byte[]> consumerIterator);
}
