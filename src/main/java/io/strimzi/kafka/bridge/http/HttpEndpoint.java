package io.strimzi.kafka.bridge.http;


import io.strimzi.kafka.bridge.Endpoint;
import io.vertx.core.http.HttpServerRequest;

public class HttpEndpoint implements Endpoint<HttpServerRequest>{
    private HttpServerRequest httpRequest;

    HttpEndpoint(HttpServerRequest httpRequest){
        this.httpRequest = httpRequest;
    }
    @Override
    public HttpServerRequest get() {
        return this.httpRequest;
    }
}
