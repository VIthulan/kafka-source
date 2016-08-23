package org.syslog_ng;

import java.util.Properties;


public class KafkaProperties {

    private String zookeeper_host;
    private String group_id_name;
    private String zookeeper_session_time_out;
    private String zookeeper_sync_time;
    private String commit_interval;
    private String consumer_timeout_interval;

    private Properties properties = new Properties();

    public KafkaProperties(String zookeeper_host, String group_id_name, String zookeeper_session_time_out,
                           String zookeeper_sync_time, String commit_interval,String consumer_timeout_interval) {
        this.zookeeper_host = zookeeper_host;
        this.group_id_name = group_id_name;
        this.zookeeper_session_time_out = zookeeper_session_time_out;
        this.zookeeper_sync_time = zookeeper_sync_time;
        this.commit_interval = commit_interval;
        this.consumer_timeout_interval = consumer_timeout_interval;
        setProperties();
    }

    public void setProperties() {
        properties.put(KafkaConstants.ZOOKEEPER_CONNECT, zookeeper_host);
        properties.put(KafkaConstants.GROUP_ID, group_id_name);
        properties.put(KafkaConstants.ZOOKEEPER_SESSION_TIMEOUT_MS, zookeeper_session_time_out);
        properties.put(KafkaConstants.ZOOKEEPER_SYNC_TIME_MS, zookeeper_sync_time);
        properties.put(KafkaConstants.ZOOKEEPER_COMMIT_INTERVAL_MS, commit_interval);
        properties.put(KafkaConstants.CONSUMER_TIMEOUT_MS,consumer_timeout_interval);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
