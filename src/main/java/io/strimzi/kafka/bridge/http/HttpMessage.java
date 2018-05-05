package io.strimzi.kafka.bridge.http;


import java.util.HashMap;

/* this class represents the content from incoming http requests. basically contains all the required fields
* needed to create a producer record while publishing or to consumer record while subscribing
*
*       __________________________________________________
*       |  field   |  GET              |         POST       |
 *      |__________|___________________|____________________|
 *      | topic    |   url param            url param       |
 *      | partition| header                 header          |
 *      | offset   | header                 header          |
 *      | group    | header                 header          |
 *      | key      | NA                     header          |
 *      | value    | NA                     body            |
 *      |          |                                        |
 *      |          |                                        |
 *      |          |                                        |
 *      |          |                                        |
 *      |__________|________________________________________|

 *
*
 */
public class HttpMessage {

   public HttpMessage(int partition, String topic, long offset, String key, byte[] value){
        this.partition = partition;
        this.topic = topic;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public HttpMessage(String topic, String group_id){
       this.topic = topic;
       this.group_id = group_id;
    }

    private int partition;

    private String topic;

    private long offset;

    private String group_id;

    private String key;

    private byte[] value;

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getRequestType() {
        return RequestType;
    }

    public void setRequestType(String requestType) {
        RequestType = requestType;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    private String RequestType;

    private String address;

}
