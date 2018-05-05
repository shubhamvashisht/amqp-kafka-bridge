package io.strimzi.kafka.bridge.http.extractors;

import io.strimzi.kafka.bridge.http.HttpMessage;
import io.vertx.core.http.HttpServerRequest;

public class GetRequestExtractor {

    private String topic;

    private String consumerGroup;

    private HttpMessage httpMessage;

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

    private void extract(HttpServerRequest request){

        topic = request.getHeader("topic");

        consumerGroup = request.getHeader("consumerid");
    };

    public HttpMessage getHttpMessage(HttpServerRequest httpServerRequest){

        extract(httpServerRequest);

        httpMessage = new HttpMessage(topic, consumerGroup);

        return httpMessage;
    }
}
