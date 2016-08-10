package org.syslog_ng;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;


public class KafkaMessageListner {
    private Properties properties;
    private String topic;
    private ConsumerConnector consumerConnector;
    private List<ConsumerIterator<byte[], byte[]>> consumerIteraror;


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
                consumerIteraror = new ArrayList<ConsumerIterator<byte[], byte[]>>();
                List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
                consumerIteraror.add(streams.get(0).iterator());
            }
        } catch (Exception e) {
            InternalMessageSender.error("Error while starting the consumer " + e.getMessage());
        }
    }

    public boolean hasNext() {
            return hasNext(consumerIteraror.get(0));
    }

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

    public String readMessages() {
            return readMessages(consumerIteraror.get(0));
    }

    public String readMessages(ConsumerIterator<byte[], byte[]> consumerIterator) {
        String message = new String(consumerIterator.next().message());
        InternalMessageSender.info("Received message : " + message);
        return message;
    }
}
