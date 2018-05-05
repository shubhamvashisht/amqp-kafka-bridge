package io.strimzi.kafka.bridge.http.extractors;

import io.strimzi.kafka.bridge.http.HttpMessage;
import io.vertx.core.http.HttpServerRequest;

public class PostRequestExtractor {

    private String topic;

    private int partition;

    private long offset;

    private byte [] value;

    private String key;

    private HttpMessage httpMessage;

    private void extract(HttpServerRequest request){

        topic = request.getHeader("topic");

        partition = Integer.parseInt(request.getHeader("partition"));

        offset = Long.parseLong(request.getHeader("offset"));

        key = request.getHeader("key");

        request.bodyHandler((buffer -> {
            value = buffer.getBytes();
        }));
    }

    public HttpMessage getHttpMessage(HttpServerRequest serverRequest){

        extract(serverRequest);

        httpMessage = new HttpMessage(partition,topic,offset,key,value);

        return httpMessage;
    }
}
