package org.syslog_ng;

import org.syslog_ng.options.InvalidOptionException;
import org.syslog_ng.options.Options;
import org.syslog_ng.options.RequiredOptionDecorator;
import org.syslog_ng.options.StringOption;


public class KafkaSourceHandler extends LogSource {
    private KafkaProperties kafkaProperties;
    private KafkaConsumer kafkaConsumer;
    private String topic;
    private String group_id;
    int count = 0;

    public KafkaSourceHandler(long l) {
        super(l);
    }

    protected boolean open() {
        System.out.println("=== Open"+count);
        count++;
        kafkaConsumer.startMessageListener();
        boolean status = kafkaConsumer.createConnection();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return status;
    }

    protected void close() {
        System.out.println("=== Close"+count);
        count++;
        kafkaConsumer.shutdown();
    }

    protected int readMessage(LogMessage logMessage) {
        System.out.println("=== ReadMessage"+count);
        count++;
        String message = kafkaConsumer.poll();
        if(message!=null){
            logMessage.setValue("MSG",message);
            return LogSource.SUCCESS;
        }
        else{
            return LogSource.NOT_CONNECTED;
        }

    }

    protected boolean isReadable() {
        System.out.println("=== isReadbale"+count);
        count++;
        return kafkaConsumer.hasNext();
    }
    protected String getStatsInstance() {
        System.out.println("=== getInstance"+count);
        count++;
        return "Kafka_source_"+group_id+"_"+topic;
    }

    protected String getPersistName() {
        System.out.println("=== getPersistName"+count);
        count++;
        return null;
    }

    protected boolean init() {
        System.out.println("=== Init"+count);
        count++;
        String zookeeper_host;
        String group_id_name;
        String topic;
        String zookeeper_session_time_out;
        String zookeeper_sync_time_out;
        String commit_interval;
        String consumer_timeout_interval;

        Options requiredOptions = new Options();
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_host)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.group_id_name)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.topic)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_session_time_out)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_sync_time_out)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.commit_interval)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.consumer_timeout_time)));
        try {
            requiredOptions.validate();
        } catch (InvalidOptionException e) {
            InternalMessageSender.error("Some options are missing");
            return false;
        }
        zookeeper_host = getOption(KafkaConstants.zookeeper_host);
        group_id_name = getOption(KafkaConstants.group_id_name);
        topic = getOption(KafkaConstants.topic);
        zookeeper_session_time_out = getOption(KafkaConstants.zookeeper_session_time_out);
        zookeeper_sync_time_out = getOption(KafkaConstants.zookeeper_sync_time_out);
        commit_interval = getOption(KafkaConstants.commit_interval);
        consumer_timeout_interval = getOption(KafkaConstants.consumer_timeout_time);

        kafkaProperties = new KafkaProperties(zookeeper_host, group_id_name, zookeeper_session_time_out,
                zookeeper_sync_time_out, commit_interval,consumer_timeout_interval);

        kafkaConsumer = new KafkaConsumer(kafkaProperties,topic);

        this.topic = topic;
        this.group_id = group_id_name;
        return true;
    }

    protected void deinit() {
        System.out.println("=== deinit"+count);
        count++;
        try {
            kafkaConsumer = null;
        } catch (Exception e){
            InternalMessageSender.error("Error occurred while deinit- "+e.getMessage());
        }
    }
}
