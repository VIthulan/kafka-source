package org.syslog_ng;

import kafka.consumer.ConsumerIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KafkaConsumerThread implements Runnable {
    private static final Log log = LogFactory.getLog(KafkaConsumerThread.class);
    private ConsumerIterator <byte[],byte[]> consumerIterator;
    int no;

    public KafkaConsumerThread (ConsumerIterator<byte[], byte[]> consumerIterator,int no){
        this.consumerIterator=consumerIterator;
        this.no = no;
    }

    public void run() {
        System.out.println("Entered into thread consuming : "+ no);
        while(consumerIterator.hasNext()){
            String message = new String(consumerIterator.next().message());
            System.out.println("Recieved : "+message);
            log.info("Message has read from kafka : "+message);
        }
        log.info("Thread exiting.."+ " : "+no);
    }
}
