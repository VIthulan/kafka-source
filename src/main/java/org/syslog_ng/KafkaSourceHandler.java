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
    public KafkaSourceHandler(long l) {
        super(l);
    }

    protected boolean open() {
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

    }

    protected int readMessage(LogMessage logMessage) {
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
        return kafkaConsumer.hasNext();
    }
    protected String getStatsInstance() {
        return "Kafka_source_"+group_id+"_"+topic;
    }

    protected String getPersistName() {
        return null;
    }

    protected boolean init() {
        String zookeeper_host;
        String group_id_name;
        String topic;
        String zookeeper_session_time_out;
        String zookeeper_sync_time_out;
        String commit_interval;

        Options requiredOptions = new Options();
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_host)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.group_id_name)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.topic)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_session_time_out)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_sync_time_out)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.commit_interval)));
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

        kafkaProperties = new KafkaProperties(zookeeper_host, group_id_name, zookeeper_session_time_out,
                zookeeper_sync_time_out, commit_interval);

        kafkaConsumer = new KafkaConsumer(kafkaProperties,topic);

        this.topic = topic;
        this.group_id = group_id_name;
        return true;
    }

    protected void deinit() {
        try {
            kafkaConsumer = null;
        } catch (Exception e){
            InternalMessageSender.error("Error occurred while deinit- "+e.getMessage());
        }
    }
}
