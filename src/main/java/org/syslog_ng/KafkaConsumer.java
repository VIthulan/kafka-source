package org.syslog_ng;


public class KafkaConsumer {

    private KafkaMessageListner kafkaMessageListner;
    private KafkaProperties kafkaProperties;
    private String topic;

    public KafkaConsumer(KafkaProperties kafkaProperties, String topic) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    /**
     * Initialize Kafka message Listner
     */
    public void startMessageListener() {
        if (kafkaMessageListner == null) {
            kafkaMessageListner = new KafkaMessageListner();
            kafkaMessageListner.init(kafkaProperties.getProperties(), topic);
        }

    }

    /**
     * Create connection with Kafka
     * @return boolean
     */
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

    /**
     * Read message from Kafka
     * @return String read message, null if no message available
     */
    public String poll() {
        return kafkaMessageListner.readMessage();
    }

    /**
     * Checks if there are any messages to consume
     * @return boolean
     */
    public boolean hasNext() {
        return kafkaMessageListner.hasNext();
    }

    /**
     * Shutdown Kafka consumer
     */
    public void shutdown(){
        kafkaMessageListner.shutdown();
    }
}
