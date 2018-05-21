package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.vertx.core.Vertx;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {

    }
}
