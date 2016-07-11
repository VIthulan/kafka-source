package org.syslog_ng;

public class KafkaSourceHandler extends LogSource {

    public KafkaSourceHandler(long l) {
        super(l);
    }

    protected boolean open() {
        return false;
    }

    protected void close() {

    }

    protected int readMessage(LogMessage logMessage) {
        return 0;
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
        return false;
    }

    protected void deinit() {

    }
}
