package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.http.converter.HttpMessageConverter;
import io.strimzi.kafka.bridge.http.extractors.GetRequestExtractor;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;

public class HttpSinkBridgeEndpoint<K,V> extends SinkBridgeEndpoint<K,V>{

    private HttpServerRequest httpServerRequest;

    private HttpConnection httpConnection;

    private HttpMessage httpMessage;

    private HttpMessageConverter httpMessageConverter;

    public HttpSinkBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }
    @Override
    public void open() {

    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.httpServerRequest = (HttpServerRequest) endpoint.get();

        httpMessage = new GetRequestExtractor().getHttpMessage(this.httpServerRequest);

        this.kafkaTopic = httpMessage.getTopic();

        this.groupId = httpMessage.getGroup_id();

        this.initConsumer();

        this.setReceivedHandler(kvKafkaConsumerRecord -> {
            System.out.println(kvKafkaConsumerRecord.record().toString());
        });

        this.subscribe();
    }
}
