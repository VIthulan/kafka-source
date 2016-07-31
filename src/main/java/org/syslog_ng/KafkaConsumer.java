package org.syslog_ng;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class KafkaConsumer {
    private static final Log log = LogFactory.getLog(KafkaConsumer.class);

    private KafkaMessageListner kafkaMessageListner;
    private KafkaProperties kafkaProperties;
    private List<String> topics;
    private int threadsCount;

    public KafkaConsumer (KafkaProperties kafkaProperties, List<String> topics, int threadsCount){
        this.kafkaProperties = kafkaProperties;
        this.topics = topics;
        this.threadsCount = threadsCount;
    }

    public void startMessageListener() {
        if(kafkaMessageListner==null){
            kafkaMessageListner = new KafkaMessageListner();
            kafkaMessageListner.init(kafkaProperties.getProperties(),topics);
        }
    }

    public Object poll() {
        try {
            if(!kafkaMessageListner.createKafkaConnector(threadsCount)){
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(kafkaMessageListner.hasMultipleTopicsToConsume()) {
            kafkaMessageListner.consumeMultipleTopics();
        } else{
           if(kafkaMessageListner.hasNext()){
               kafkaMessageListner.readMessages();
           }
        }
        return null;
    }

    /**
     * It will consume messages from the kafka server
     *//*
    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
        while (consumerIterator.hasNext()) {
            String message = new String(consumerIterator.next().message());
            log.info("Message received in thread " + threadNumber + " : " + message);
        }
        log.debug("Shutting down thread " + threadNumber);
    }*/
}
