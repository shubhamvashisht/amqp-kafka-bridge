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

import io.netty.handler.codec.http.HttpMessage;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class HttpSinkBridgeEndpoint<V, K> extends SinkBridgeEndpoint<V, K> {

    private HttpServerRequest httpServerRequest;

    private RequestExtractor requestExtractor;

    private HttpServerResponse httpServerResponse;

    private int consumerId;

    public HttpSinkBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties) {
        super(vertx, bridgeConfigProperties);
        requestExtractor = new RequestExtractor();
    }

    @Override
    public void open() {

    }

    @Override
    public void handle(Endpoint<?> endpoint) {

    }private HttpMessage httpMessage;

    @Override
    public void consumerCreateHandler(Endpoint<?> endpoint, int consumerId) {

        this.consumerId = consumerId;
        //incoming http reuqest
        httpServerRequest = (HttpServerRequest) endpoint.get();

        httpServerResponse = httpServerRequest.response();

        //extracts fields from request params
        requestExtractor.extract(httpServerRequest);

        this.groupId = requestExtractor.getConsumerGroup();

        this.createConsumer();

        sendHttpResponse(responseForCreateConsumer(String.valueOf(consumerId)));
    }

    @Override
    public void consumerSubscribeHandler(Endpoint<?> endpoint) {
        httpServerRequest = (HttpServerRequest) endpoint.get();
        httpServerResponse = httpServerRequest.response();
        requestExtractor.extract(httpServerRequest);

        this.kafkaTopic = requestExtractor.getTopic();

        this.partition = Integer.parseInt(requestExtractor.getPartition());

        this.subscribeTo();

        sendHttpResponse(responseForSubscription(String.valueOf(consumerId), this.kafkaTopic));
    }

    @Override
    public void consumerConsumeHandler(Endpoint<?> endpoint) {
        httpServerRequest = (HttpServerRequest) endpoint.get();
        httpServerResponse = httpServerRequest.response();

        requestExtractor.extract(httpServerRequest);

        //max poll time
        this.pollTimeout = Long.parseLong(requestExtractor.getPollTimeOut());

        //max records to fetch
        this.maxRecords = Integer.parseInt(requestExtractor.getMaxRecords());

        this.setHttpReceivedHandler(this::receiveRecords);

        this.consume();


    }

    private void receiveRecords(ConsumerRecords<V, K> consumerRecords){
        JsonArray jsonArray = new JsonArray();
        for (ConsumerRecord<V, K> consumerRecord : consumerRecords) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.put("record",consumerRecord.toString());
            jsonArray.add(jsonObject);
        }

        sendHttpResponse(jsonArray.encode());
    }

    private void sendHttpResponse(String response){
        this.httpServerResponse.putHeader("Content-length", String.valueOf(response.length()))
                .write(response)
                .end();
    }

    private String responseForCreateConsumer(String consumerId){
        JsonObject status = new JsonObject();
        status.put("status","success");
        status.put("consumer_id",consumerId);
        return status.encode();
    }

    private String responseForSubscription(String consumerId, String topic){
        JsonObject status = new JsonObject();
        status.put("status","success");
        status.put("consumer_id", consumerId);
        status.put("topic", topic);
        return status.encode();
    }
}
