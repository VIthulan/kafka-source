package org.syslog_ng;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KafkaConsumer implements Runnable {
    private KafkaStream stream;
    private int threadNumber;
    private static final Log log = LogFactory.getLog(KafkaConsumer.class);

    public KafkaConsumer(KafkaStream stream, int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
    }

    /**
     * It will consume messages from the kafka server
     */
    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
        while (consumerIterator.hasNext()) {
            String message = new String(consumerIterator.next().message());
            log.info("Message received in thread " + threadNumber + " : " + message);
        }
        log.debug("Shutting down thread " + threadNumber);
    }
}
