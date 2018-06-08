package io.strimzi.kafka.bridge.http;

public class HttpMessage {

    HttpMessage(){

    }

    public HttpMessage(String topic, int partition, long timeStamp, String key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.timeStamp = timeStamp;
        this.key = key;
        this.value = value;
    }

    private String topic;

    private int partition;

    private long timeStamp;

    private String key;

    private byte[] value;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
