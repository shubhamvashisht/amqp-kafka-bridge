package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {

    private HttpConnection httpConnection;

    private HttpServerRequest httpServerRequest;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.httpServerRequest = (HttpServerRequest) endpoint.get();
        this.httpConnection = this.httpServerRequest.connection();

        this.httpServerRequest.handler((buffer -> {
            System.out.println(buffer.length());
        }));
    }
}
