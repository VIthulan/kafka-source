package org.syslog_ng;


public class KafkaConsumer {

    private KafkaMessageListner kafkaMessageListner;
    private KafkaProperties kafkaProperties;
    private String topic;

    public KafkaConsumer(KafkaProperties kafkaProperties, String topic) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    public void startMessageListener() {
        if (kafkaMessageListner == null) {
            kafkaMessageListner = new KafkaMessageListner();
            kafkaMessageListner.init(kafkaProperties.getProperties(), topic);
        }

    }

    public boolean createConnection() {
        try {
            if (!kafkaMessageListner.createKafkaConnector()) {
                return false;
            } else {
                InternalMessageSender.debug("Connection created with Kafka");
                return true;
            }
        } catch (Exception e) {
            InternalMessageSender.error("Exception at Creating connection "+e.getMessage());
            return false;
        }
    }

    public String poll() {
        return kafkaMessageListner.readMessage();
    }

    public boolean hasNext() {
        return kafkaMessageListner.hasNext();
    }

}
