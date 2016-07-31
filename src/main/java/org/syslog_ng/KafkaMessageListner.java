package org.syslog_ng;

import kafka.consumer.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaMessageListner extends AbstractKafkaMessageListner {
    private static final Log log = LogFactory.getLog(KafkaMessageListner.class);
    private Properties properties;
    private ExecutorService executor;

    /**
     * These are default values, it can be configured in main method
     */
    private String zookeeper_session_time_out = "400";
    private String zookeeper_sync_time_out = "200";
    private String commit_interval = "1000";




    @Override
    public void init(Properties properties, List<String> topic) {
        this.properties = properties;
        log.info("Kafka consumer properties are set successfully");
        this.topics = topic;
    }

    /**
     * Setting up zookeeper configurations
     *
     * @param zookeeper_session_time_out Zookeeper session timeout. If the consumer fails to heartbeat to zookeeper
     *                                   for this period of time it is considered dead and a rebalance will occur.
     * @param zookeeper_sync_time_out    How far a ZK follower can be behind a ZK leader
     * @param commit_interval            The frequency in ms that the consumer offsets are committed to zookeeper.
     */
    public void initZooKeeper(String zookeeper_session_time_out, String zookeeper_sync_time_out,
                              String commit_interval) {
        this.zookeeper_session_time_out = zookeeper_session_time_out;
        this.zookeeper_sync_time_out = zookeeper_sync_time_out;
        this.commit_interval = commit_interval;
    }


    /**
     * Creating consumer connector
     *
     * @param threadsCount number of threads to run
     * @return true if connector created successfully
     * @throws Exception
     */
    @Override
    public boolean createKafkaConnector(int threadsCount) throws Exception {
        boolean isCreated = false;
        try {
            if (consumerConnector == null) {
                consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
                log.info("Consumer connector created successfully");
                this.threadCount = threadsCount;
                start(threadsCount);
            }
            isCreated = true;
        } catch (Exception e) {
            log.error("Error while creating consumer connector " + e.getMessage());
        }
        return isCreated;
    }

    /**
     * Start the consumer threads
     *
     * @param threadsCount number of threads to run
     * @throws Exception
     */
    @Override
    public void start(int threadsCount) throws Exception {
        /*try {
            createKafkaConnector(threadsCount);
        } catch (Exception e) {
            log.error("Error while creating consumer connector " + e.getMessage());
        }*/
      //  this.threadCount = threadsCount;
        try {
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            if (topics != null && topics.size() > 0) {
                System.out.println("topics count " + topics.size());
                for (String topic : topics) {
                    topicCountMap.put(topic, threadCount);
                }

                Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
                        .createMessageStreams(topicCountMap);
                consumerIteraror = new ArrayList<ConsumerIterator<byte[], byte[]>>();
                for(String topic : topics) {
                    List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
                    startConsumers(streams);
                }
                /*executor = Executors.newFixedThreadPool(threadCount);
                log.info("Thread pool with " + threadsCount + " thread/s is initiated");
                int threadNumber = 0;
                for (final KafkaStream stream : streams) {

                    //System.out.println("Thread number " + threadNumber);
                    *//*executor.submit(new KafkaConsumer(stream, threadNumber));
                    threadNumber++;*//*
                }*/
            }
        } catch (Exception e) {
            log.error("Error while starting the consumer " + e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        if (consumerIteraror.size() == 1) {
            return hasNext(consumerIteraror.get(0));
        } else {
            log.debug("There are multiple topics to consume from not a single topic,");
        }
        return false;
    }

    public boolean hasNext(ConsumerIterator<byte[], byte[]> consumerIterator){
        try {
            return consumerIterator.hasNext();
        } catch (ConsumerTimeoutException e) {
            //exception ignored
            if (log.isDebugEnabled()) {
                log.debug("Topic has no new messages to consume.");
            }
            return false;
        } catch (Exception e) {
            //Message Listening thread is interrupted during server shutdown.
            if (log.isDebugEnabled()) {
                log.debug("Kafka listener is interrupted by server shutdown.", e);
            }
            return false;
        }
    }

    /**
     * This implementation is for multiple topics
     *
     * @param streams
     */
    protected void startConsumers(List<KafkaStream<byte[], byte[]>> streams) {
        if (streams.size() >= 1) {
            consumerIteraror.add(streams.get(0).iterator());
        }
    }

    public boolean hasMultipleTopicsToConsume(){
        if (consumerIteraror.size() > 1) {
            return true;
        } else {
            return false;
        }
    }

    public void consumeMultipleTopics() {
        for (ConsumerIterator<byte[], byte[]> consumerIte : consumerIteraror) {
            if (hasNext(consumerIte)) {
                readMessages(consumerIte);
            }
        }
    }

    public void readMessages(){
        if(consumerIteraror.size()==1){
            readMessages(consumerIteraror.get(0));
        }
        else{
            log.error("Multiple topics available, can't consume");
        }
    }
    @Override
    public void readMessages(ConsumerIterator<byte[], byte[]> consumerIterator) {
        if (consumerIteraror.size() == 1) {
            String message = new String(consumerIterator.next().message());
            log.info("Message has read from kafka : "+message);
        } else {
            log.debug("There are multiple topics to consume from not a single topic");
        }
    }


}
