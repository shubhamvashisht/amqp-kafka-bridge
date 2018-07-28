/*
 * Copyright 2018 Red Hat Inc.
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

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class HttpSinkBridgeEndpoint<V, K> extends SinkBridgeEndpoint<V, K> {

    private HttpServerRequest httpServerRequest;

    private String consumerName;

    private Handler<String> consumerIdHandler;

    HttpSinkBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties httpBridgeConfigProperties) {
        super(vertx, httpBridgeConfigProperties);
    }

    @Override
    public void open() {

    }

    @Override
    public void handle(Endpoint<?> endpoint) {

        httpServerRequest = (HttpServerRequest) endpoint.get();

        RequestType requestType = RequestIdentifier.getRequestType(httpServerRequest);

        switch (requestType){

            case SUBSCRIBE:

                httpServerRequest.bodyHandler(buffer -> {
                    this.topic = buffer.toJsonObject().getString("topic");
                    this.partition = buffer.toJsonObject().getInteger("partition");
                    if (buffer.toJsonObject().containsKey("offset")) {
                        this.offset = Long.parseLong(buffer.toJsonObject().getString("offset"));
                    }
                    this.kafkaTopic = this.topic;
                    this.setSubscribeHandler(subscribeResult -> {
                        if (subscribeResult.succeeded()) {
                            sendConsumerSubscriptionResponse(httpServerRequest.response());
                        }
                    });
                    this.subscribe(false);
                });

                break;

            case INVALID:
                log.info("invalid request");
        }

    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<String> handler) {
        httpServerRequest = (HttpServerRequest) endpoint.get();

        RequestType requestType = RequestIdentifier.getRequestType(httpServerRequest);

        switch (requestType){

            case CREATE:

                //set group id of consumer
                groupId = PathParamsExtractor.getConsumerConsumerCreationParams(httpServerRequest).get("group-id");
                httpServerRequest.bodyHandler(buffer -> {
                    if (buffer.toJsonObject().containsKey("name")) {
                        consumerName = buffer.toJsonObject().getString("name");
                    }
                    //in case name is not mentioned by the client explicitly, assign a random name.
                    else {
                        consumerName = "kafka-bridge-consumer-";
                        consumerName += UUID.randomUUID().toString();
                    }
                    //construct consumer instance id
                    consumerInstanceId = consumerName;
                    //construct base URI for consumer
                    String requestUri = httpServerRequest.absoluteURI();
                    if (!httpServerRequest.path().endsWith("/")){
                        requestUri += "/";
                    }
                    consumerBaseUri = requestUri+"instances/"+super.consumerInstanceId;

                    //create the consumer
                    this.initConsumer(false);

                    this.consumerIdHandler = handler;
                    this.consumerIdHandler.handle(consumerInstanceId);

                    //send consumer instance id(name) and base URI as response
                    sendConsumerCreationResponse(httpServerRequest.response(), consumerInstanceId, consumerBaseUri);
                });
                break;

            case INVALID:
                log.info("invalid request");
        }
    }

    private void sendConsumerCreationResponse(HttpServerResponse response, String instanceId, String uri) {
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("instance_id", instanceId);
        jsonResponse.put("base_uri", uri);

        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }

    private void sendConsumerSubscriptionResponse(HttpServerResponse response) {
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("subscription_status", "subscribed");

        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }

}
