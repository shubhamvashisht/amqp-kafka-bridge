package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.vertx.core.Vertx;

public class HttpSinkBridgeEndpoint<K,V> extends SinkBridgeEndpoint<K,V>{

    public HttpSinkBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }
    @Override
    public void open() {

    }

    @Override
    public void handle(Endpoint<?> endpoint) {

    }
}
