/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.vertx.core.http.HttpServerRequest;

public class RequestExtractor {

    private String consumerName;

    private String offset;

    private String topic;

    private String consumerGroup;

    private String partition;

    private String pollTimeOut;

    private String maxRecords;

    private String key;

    private String timeStamp;

    private String value;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getPollTimeOut() {
        return pollTimeOut;
    }

    public void setPollTimeOut(String pollTimeOut) {
        this.pollTimeOut = pollTimeOut;
    }

    public String getMaxRecords() {
        return maxRecords;
    }

    public void setMaxRecords(String maxRecords) {
        this.maxRecords = maxRecords;
    }

    public void extract(HttpServerRequest request){

        //int topicIndex = request.path().indexOf("/", request.path().indexOf("/")+1);

        //topic+1 to skip initial "/" from path
        //topic = request.path().substring(topicIndex+1);

        key = request.getHeader("key");

        topic = request.getHeader("topic");

        consumerGroup = request.getHeader("consumer_id");

        partition = request.getHeader("partition");

        pollTimeOut = request.getHeader("poll_time_out");

        maxRecords = request.getHeader("max_records");

        timeStamp = request.getHeader("time_Stamp");

        value = request.getHeader("value");

    };

    public HttpMessage getHttpMessage(){
        return new HttpMessage(topic, Integer.parseInt(partition), Long.parseLong(timeStamp), key, value.getBytes());
    }
}