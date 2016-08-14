package org.syslog_ng;

import java.util.Properties;


public class KafkaProperties {

    private static String zookeeper_connect = KafkaConstants.ZOOKEEPER_CONNECT;
    private static String group_id = KafkaConstants.GROUP_ID;
    private static String zookeeper_session_timeout_ms = KafkaConstants.ZOOKEEPER_SESSION_TIMEOUT_MS;
    private static String zookeeper_sync_time_ms = KafkaConstants.ZOOKEEPER_SYNC_TIME_MS;
    private static String auto_commit_interval_ms = KafkaConstants.ZOOKEEPER_COMMIT_INTERVAL_MS;

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
        properties.put(zookeeper_connect, zookeeper_host);
        properties.put(group_id, group_id_name);
        properties.put(zookeeper_session_timeout_ms, zookeeper_session_time_out);
        properties.put(zookeeper_sync_time_ms, zookeeper_sync_time);
        properties.put(auto_commit_interval_ms, commit_interval);
        properties.put(KafkaConstants.CONSUMER_TIMEOUT_MS,consumer_timeout_interval);
        /*properties.put(KafkaConstants.AUTO_OFFSET_RESET,"smallest");
        properties.put(KafkaConstants.CONSUMER_TIMEOUT_MS,"10");*/
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
