package org.syslog_ng;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;


public class KafkaMessageListner {
    private Properties properties;
    private String topic;
    private ConsumerConnector consumerConnector;
    private List<ConsumerIterator<byte[], byte[]>> consumerIterator;


    /**
     * Initializes required variables
     * @param properties Kafka property set by user
     * @param topic Kafka topic to consume
     */
    public void init(Properties properties, String topic) {
        this.properties = properties;
        InternalMessageSender.info("Kafka consumer properties are set successfully");
        this.topic = topic;
    }

    /**
     * Creating consumer connector
     *
     * @return true if connector created successfully
     * @throws Exception
     */
    public boolean createKafkaConnector() throws Exception {
        boolean isCreated = false;
        try {
            if (consumerConnector == null) {
                consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
                InternalMessageSender.info("Consumer connector created successfully");
                start();
            }
            isCreated = true;
        } catch (Exception e) {
            InternalMessageSender.error("Error while creating consumer connector " + e.getMessage());
        }
        return isCreated;
    }

    /**
     * Start the consumer threads
     *
     * @throws Exception
     */
    public void start() throws Exception {
        try {
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            if (topic != null) {
                topicCountMap.put(topic, 1);
                Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
                        .createMessageStreams(topicCountMap);
                consumerIterator = new ArrayList<ConsumerIterator<byte[], byte[]>>();
                List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
                consumerIterator.add(streams.get(0).iterator());
            }
        } catch (Exception e) {
            InternalMessageSender.error("Error while starting the consumer " + e.getMessage());
        }
    }

    public boolean hasNext() {
            return hasNext(consumerIterator.get(0));
    }

    /**
     * Checks if there are any messages to consume
     * @param consumerIterator Consumer iterator of Kafka consumer
     * @return boolean
     */
    public boolean hasNext(ConsumerIterator<byte[], byte[]> consumerIterator) {
        try {
            return consumerIterator.hasNext();  //toDo this hasnext returns true / waits for new kafka message
        } catch (ConsumerTimeoutException e) {
            InternalMessageSender.info("There is no no new messages to consume");
            InternalMessageSender.info("Consumer timed out - Exiting");
            return false;
        } catch (Exception e) {
            InternalMessageSender.debug("Kafka listener is interrupted " + e.getMessage());
            return false;
        }
    }

    public String readMessage() {
            return readMessage(consumerIterator.get(0));
    }

    /**
     * Reads message from Kafka
     * @param consumerIterator Consumer iterator of Kafka
     * @return String message
     */
    public String readMessage(ConsumerIterator<byte[], byte[]> consumerIterator) {
        try{
            String message = new String(consumerIterator.next().message());
            InternalMessageSender.info("Received message : " + message);
            return message;
        } catch (ConsumerTimeoutException exception){
            InternalMessageSender.debug("Consumer timeout exception occurred -"+exception.getMessage());
        }
        return null;
    }

    /**
     * Shut down Kafka consumer
     */
    public void shutdown(){
       consumerConnector.shutdown();
    }
}
