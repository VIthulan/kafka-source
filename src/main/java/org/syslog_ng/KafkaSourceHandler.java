package org.syslog_ng;

import org.syslog_ng.options.Options;
import org.syslog_ng.options.RequiredOptionDecorator;
import org.syslog_ng.options.StringOption;

import java.util.ArrayList;
import java.util.List;

public class KafkaSourceHandler extends LogSource {
    /**
     * Hardcoded options
     */
    private static final String zookeeper_host = "localhost:2181";
    private static final String group_id_name = "groupid11";
    private static final int threads_number = 2;
    private static final String topic = "testtopic";
    private static final String zookeeper_session_time_out = "400";
    private static final String zookeeper_sync_time_out = "200";
    private static final String commit_interval = "1000";

    private KafkaProperties kafkaProperties;
    private KafkaConsumer kafkaConsumer;

    public KafkaSourceHandler(long l) {
        super(l);
    }

    protected boolean open() {
        return kafkaConsumer.createConnection();
    }

    protected void close() {

    }

    protected int readMessage(LogMessage logMessage) {
        boolean status = kafkaConsumer.poll();
        if(status){
            return 1;
        }
        else
            return -1;

    }

    protected void ack(LogMessage logMessage) {

    }

    protected void nack(LogMessage logMessage) {

    }

    protected boolean isReadable() {
        return false;
    }

    protected String getStatsInstance() {
        return null;
    }

    protected String getPersistName() {
        return null;
    }

    protected String getCursor() {
        return null;
    }

    protected boolean seekToCursor(String s) {
        return false;
    }

    protected boolean init() {
        List<String> topics = new ArrayList<String>();
        topics.add(topic);

        kafkaProperties = new KafkaProperties(zookeeper_host, group_id_name, zookeeper_session_time_out,
                zookeeper_sync_time_out, commit_interval);

        kafkaConsumer = new KafkaConsumer(kafkaProperties,topics,threads_number);

        //toDo Need to change StringOptions constructor for LogSource
       /* Options requiredOptions = new Options();
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,"zoo")));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.)));
        requiredOptions.put(new RequiredOptionDecorator(new StringOption(this,KafkaConstants.)));*/
        return true;
    }

    protected void deinit() {

    }
}
