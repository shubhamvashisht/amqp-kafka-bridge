package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.vertx.core.http.HttpServerRequest;

public class HttpEndpoint implements Endpoint<HttpServerRequest> {

    private HttpServerRequest httpServerRequest;

    public HttpEndpoint(HttpServerRequest httpServerRequest){
        this.httpServerRequest = httpServerRequest;
    }

    @Override
    public HttpServerRequest get() {
        return this.httpServerRequest;
    }
}
