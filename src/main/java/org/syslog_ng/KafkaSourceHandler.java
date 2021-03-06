package org.syslog_ng;

import org.syslog_ng.options.*;


public class KafkaSourceHandler extends LogSource {
    private KafkaProperties kafkaProperties;
    private KafkaConsumer kafkaConsumer;
    private String topic;
    private String group_id;
    private boolean isOpened = false;
    private boolean isFirstOpen = true;

    public KafkaSourceHandler(long l) {
        super(l);
    }

    protected boolean open() {
        kafkaConsumer.startMessageListener();
        boolean status = kafkaConsumer.createConnection();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            InternalMessageSender.error("Error occurred while creating connection with Kafka");
        }
        isOpened = status;
        isFirstOpen  = false;
        return status;
    }

    protected void close() {
        kafkaConsumer.shutdown();
    }

    protected int readMessage(LogMessage logMessage) {
        String message = kafkaConsumer.poll();
        if(message!=null){
            logMessage.setValue("MSG",message);
            return LogSource.SUCCESS;
        }
        else{
            isOpened = false;
            return LogSource.NOT_CONNECTED;
        }
    }

    protected boolean isReadable() {
        isOpened = kafkaConsumer.hasNext();
        return isOpened;
    }

    protected boolean isOpened() {
        if(isFirstOpen){
            return isOpened;
        }
        else {
            if(!isOpened){
                reInitiateKafka();
            }
            return isOpened;
        }

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
        String consumer_timeout_interval;

        Options requiredOptions = new Options();
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_host)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.group_id_name)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.topic)));
        requiredOptions.put(new IntegerOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_session_time_out,"400")));
        requiredOptions.put(new IntegerOptionDecorator(new StringOption(this,KafkaConstants.zookeeper_sync_time_out,"200")));
        requiredOptions.put(new IntegerOptionDecorator(new StringOption(this,KafkaConstants.commit_interval,"1000")));
        requiredOptions.put(new IntegerOptionDecorator(new StringOption(this,KafkaConstants.consumer_timeout_time,"10000")));
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
        try {
            kafkaConsumer = null;
        } catch (Exception e){
            InternalMessageSender.error("Error occurred while deinit- "+e.getMessage());
        }
    }
    private void reInitiateKafka(){
        kafkaConsumer.shutdown();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            InternalMessageSender.error("Error - "+e.getMessage());
        }
        kafkaConsumer = null;
        kafkaConsumer = new KafkaConsumer(kafkaProperties,topic);
    }

}
